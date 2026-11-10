package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.Call
import okhttp3.ConnectionPool
import okhttp3.Dispatcher
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.InterruptedIOException
import java.io.IOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapterImpl::class.java)
        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
        private const val ADMISSION_MARGIN_MS = 100L
        private const val RATE_WINDOW_GUARD_MS = 10L
        private const val WARMUP_SAMPLES = 100
        private const val WARMUP_FACTOR = 2.0
        private const val RETRY_PARALLELISM = 2
        private val attemptExecutor = Executors.newCachedThreadPool { r ->
            Thread(r, "payment-attempt").apply { isDaemon = true }
        }
        private val slotReleaser = Executors.newSingleThreadScheduledExecutor { r ->
            Thread(r, "payment-slot-releaser").apply { isDaemon = true }
        }
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val expectedProcessingMs = properties.averageProcessingTime.toMillis()
    private val processingSamples = ConcurrentLinkedDeque<Long>()
    private val maxProcessingSamples = 512
    private val parallelPermits = properties.parallelRequests.coerceAtLeast(1)
    private val inFlight = AtomicInteger(0)
    private val processing = AtomicInteger(0)

    @Volatile
    private var processingP99Ms = (expectedProcessingMs * WARMUP_FACTOR).toLong().coerceAtLeast(1)

    @Volatile
    private var processingAvgMs = processingP99Ms

    @Volatile
    private var processingP50Ms = expectedProcessingMs.coerceAtLeast(1)

    private val parallelRequestsSemaphore = Semaphore(parallelPermits, true)

    private val rateLimiter = SlidingWindowRateLimiter(
        properties.rateLimitPerSec.toLong(),
        Duration.ofMillis(1000 + RATE_WINDOW_GUARD_MS),
    )

    private class PaymentAttempt(
        val latestStart: Long,
        @Volatile var ratePermitHeld: Boolean = false,
    ) {
        val transactionId: UUID = UUID.randomUUID()
        val createdAt = System.currentTimeMillis()
        @Volatile var submittedAt: Long? = null
        @Volatile var call: Call? = null
        @Volatile var cancelled = false

        fun cancel() {
            cancelled = true
            call?.cancel()
        }
    }

    private class RateWaitTimeout : IOException("Deadline exceeded while waiting for rate limit")

    private val client = OkHttpClient.Builder()
        .addNetworkInterceptor { chain ->
            val attempt = checkNotNull(chain.request().tag(PaymentAttempt::class.java))
            if (attempt.submittedAt != null) {
                processing.decrementAndGet()
                attempt.submittedAt = null
            }
            if (attempt.ratePermitHeld) {
                attempt.ratePermitHeld = false
            } else if (!awaitRatePermit(attempt.latestStart)) {
                throw RateWaitTimeout()
            }
            attempt.submittedAt = System.currentTimeMillis()
            processing.incrementAndGet()
            chain.proceed(chain.request())
        }
        .dispatcher(
            Dispatcher().apply {
                maxRequests = parallelPermits
                maxRequestsPerHost = parallelPermits
            }
        )
        .connectionPool(ConnectionPool(parallelPermits, 1L, TimeUnit.MINUTES))
        .connectTimeout(Duration.ofSeconds(5))
        .readTimeout(Duration.ZERO)
        .writeTimeout(Duration.ofSeconds(5))
        .build()

    override fun canAccept(deadline: Long, queuedRequests: Int): Boolean {
        if (properties.rateLimitPerSec <= 0 || properties.parallelRequests <= 0) {
            return false
        }
        val processingMs = processingP99Ms
        while (true) {
            val now = System.currentTimeMillis()
            val current = inFlight.get()
            val active = processing.get()
            val pending = (current - active).coerceAtLeast(0) + 1
            val freeSlots = (parallelPermits - active).coerceAtLeast(0)
            val parallelWaitMs = (pending - freeSlots).coerceAtLeast(0) *
                processingAvgMs.toDouble() / parallelPermits
            val rateWaitMs = rateLimiter.millisUntilAvailable(pending)
            val predictedCompletion =
                now + maxOf(parallelWaitMs, rateWaitMs.toDouble()) + processingMs + ADMISSION_MARGIN_MS
            if (predictedCompletion >= deadline) {
                return false
            }
            if (inFlight.compareAndSet(current, current + 1)) {
                return true
            }
        }
    }

    override fun releaseAdmission() {
        inFlight.updateAndGet { current -> if (current > 0) current - 1 else 0 }
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        var permitHeld = false
        try {
            logger.debug("[$accountName] Submitting payment request for payment $paymentId")
            val now = System.currentTimeMillis()

            if (now >= latestStart(deadline)) {
                logger.debug("[$accountName] Payment $paymentId has insufficient time before deadline")
                reject(paymentId, UUID.randomUUID(), paymentStartedAt, "Insufficient time before deadline")
                return
            }

            val slotWaitMs = (latestStart(deadline) - System.currentTimeMillis()).coerceAtLeast(0)
            val acquired = try {
                parallelRequestsSemaphore.tryAcquire(slotWaitMs, TimeUnit.MILLISECONDS)
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                false
            }
            if (!acquired) {
                logger.debug("[$accountName] Payment $paymentId failed to acquire parallel slot")
                reject(paymentId, UUID.randomUUID(), paymentStartedAt, "Failed to acquire parallel slot before deadline")
                return
            }
            permitHeld = true

            var outcome = executeAttempt(paymentId, amount, PaymentAttempt(latestStart(deadline)), deadline)
            logSubmission(paymentId, outcome)
            var slotOutcome = outcome
            var attempts = 1

            while (!outcome.result && outcome.retryable && System.currentTimeMillis() < retryLatestStart(deadline)) {
                val roundLatestStart = retryLatestStart(deadline)
                if (!awaitRatePermit(roundLatestStart)) break
                val round = mutableListOf(PaymentAttempt(roundLatestStart, ratePermitHeld = true))
                while (round.size < RETRY_PARALLELISM && parallelRequestsSemaphore.tryAcquire()) {
                    if (!rateLimiter.tick()) {
                        parallelRequestsSemaphore.release()
                        break
                    }
                    round.add(PaymentAttempt(roundLatestStart, ratePermitHeld = true))
                }

                logger.debug("[$accountName] Retrying payment $paymentId with ${round.size} parallel attempt(s) after: ${outcome.reason}")
                val outcomes = runRound(paymentId, amount, round, deadline)
                attempts += outcomes.size
                outcomes.forEach { logSubmission(paymentId, it) }
                slotOutcome = outcomes.first()
                outcomes.drop(1).forEach { releaseSlot(it) }

                outcome = outcomes.firstOrNull { it.result }
                    ?: outcomes.firstOrNull { it.retryable }
                    ?: outcomes.last()
            }

            releaseSlot(slotOutcome)
            permitHeld = false

            if (!outcome.result) logger.warn("[$accountName] Payment $paymentId failed after $attempts attempt(s): ${outcome.reason}")
            paymentESService.update(paymentId) {
                it.logProcessing(outcome.result, outcome.finishedAt, outcome.transactionId, reason = outcome.reason)
            }
        } finally {
            if (permitHeld) {
                parallelRequestsSemaphore.release()
            }
            releaseAdmission()
        }
    }

    private class AttemptOutcome(
        val transactionId: UUID,
        val result: Boolean,
        val reason: String?,
        val retryable: Boolean,
        val timedOut: Boolean,
        val cancelled: Boolean,
        val createdAt: Long,
        val submittedAt: Long?,
        val elapsed: Long?,
        val finishedAt: Long,
    )

    private fun runRound(paymentId: UUID, amount: Int, round: List<PaymentAttempt>, deadline: Long): List<AttemptOutcome> {
        val futures = round.map { attempt ->
            CompletableFuture.supplyAsync({ executeAttempt(paymentId, amount, attempt, deadline) }, attemptExecutor)
        }
        val pending = futures.toMutableList()
        while (pending.isNotEmpty()) {
            CompletableFuture.anyOf(*pending.toTypedArray()).join()
            val done = pending.filter { it.isDone }
            pending.removeAll(done)
            if (done.any { it.join().result }) {
                round.forEach { it.cancel() }
                break
            }
        }
        return futures.map { it.join() }
    }

    private fun logSubmission(paymentId: UUID, outcome: AttemptOutcome) {
        val submittedAt = outcome.submittedAt ?: outcome.finishedAt
        paymentESService.update(paymentId) {
            it.logSubmission(
                success = outcome.submittedAt != null,
                outcome.transactionId,
                submittedAt,
                Duration.ofMillis(submittedAt - outcome.createdAt)
            )
        }
    }

    private fun releaseSlot(outcome: AttemptOutcome) {
        val elapsed = outcome.elapsed
        val releaseDelay = if ((outcome.timedOut || outcome.cancelled) && elapsed != null) processingP99Ms - elapsed else 0L
        if (releaseDelay > 0) {
            slotReleaser.schedule({ parallelRequestsSemaphore.release() }, releaseDelay, TimeUnit.MILLISECONDS)
        } else {
            parallelRequestsSemaphore.release()
        }
    }

    private fun executeAttempt(paymentId: UUID, amount: Int, attempt: PaymentAttempt, deadline: Long): AttemptOutcome {
        val transactionId = attempt.transactionId
        var result = false
        var reason: String? = null
        var retryable = false
        var timedOut = false

        try {
            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
                tag(PaymentAttempt::class.java, attempt)
            }.build()

            val call = client.newCall(request)
            val httpTimeout = (deadline - System.currentTimeMillis() - 50).coerceAtLeast(1L)
            call.timeout().timeout(httpTimeout, TimeUnit.MILLISECONDS)
            attempt.call = call
            if (attempt.cancelled) call.cancel()

            call.execute().use { response ->
                val responseBody = response.body?.string()
                val body = try {
                    mapper.readValue(responseBody, ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: $responseBody")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                }
                logger.debug("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
                result = body.result
                reason = body.message
                retryable = !result && (response.isSuccessful || response.code == 429 || response.code >= 500)
            }
        } catch (e: Exception) {
            if (e !is RateWaitTimeout && !attempt.cancelled) {
                logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
            }
            timedOut = e is InterruptedIOException && !attempt.cancelled
            retryable = e is IOException && e !is RateWaitTimeout && e !is InterruptedIOException && !attempt.cancelled
            reason = when {
                attempt.cancelled -> "Cancelled: another attempt succeeded"
                e is RateWaitTimeout -> e.message
                e is SocketTimeoutException || e is InterruptedIOException -> "HTTP request timeout after ${e.message}"
                else -> e.message ?: "Unknown error"
            }
        }

        val finishedAt = System.currentTimeMillis()
        val submittedAt = attempt.submittedAt
        val elapsed = submittedAt?.let { finishedAt - it }
        if (elapsed != null) {
            if (!attempt.cancelled) recordProcessing(elapsed)
            processing.decrementAndGet()
        }
        return AttemptOutcome(
            transactionId, result, reason, retryable, timedOut, attempt.cancelled,
            attempt.createdAt, submittedAt, elapsed, finishedAt
        )
    }

    private fun latestStart(deadline: Long) = deadline - processingP99Ms - ADMISSION_MARGIN_MS

    private fun retryLatestStart(deadline: Long) = deadline - processingP50Ms - ADMISSION_MARGIN_MS

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
    override fun parallelism() = parallelPermits
    override fun admittedCount() = inFlight.get()
    override fun rateWaiters() = rateLimiter.waitingCount()

    private fun awaitRatePermit(latestStart: Long): Boolean {
        val remaining = latestStart - System.currentTimeMillis()
        if (remaining <= 0) return false
        return try {
            rateLimiter.awaitPermit(Duration.ofMillis(remaining))
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            false
        }
    }

    private fun reject(paymentId: UUID, transactionId: UUID, paymentStartedAt: Long, reason: String) {
        logger.warn("[$accountName] Payment $paymentId rejected: $reason")
        val now = System.currentTimeMillis()
        paymentESService.update(paymentId) {
            it.logSubmission(success = false, transactionId, now, Duration.ofMillis(now - paymentStartedAt))
            it.logProcessing(false, now, transactionId, reason = reason)
        }
    }

    private fun recordProcessing(durationMillis: Long) {
        processingSamples.addLast(durationMillis.coerceAtLeast(1))
        while (processingSamples.size > maxProcessingSamples) {
            processingSamples.pollFirst()
        }
        val sorted = processingSamples.toList().sorted()
        if (sorted.isEmpty()) return
        val p99 = sorted[((sorted.size - 1) * 0.99).toInt()].coerceAtLeast(1)
        val p50 = sorted[(sorted.size - 1) / 2].coerceAtLeast(1)
        if (sorted.size < WARMUP_SAMPLES) {
            processingP99Ms = maxOf(p99, (expectedProcessingMs * WARMUP_FACTOR).toLong())
            processingAvgMs = processingP99Ms
            processingP50Ms = maxOf(p50, expectedProcessingMs)
        } else {
            processingP99Ms = p99
            processingP50Ms = p50
            processingAvgMs = sorted.average().toLong().coerceAtLeast(1)
        }
    }
}

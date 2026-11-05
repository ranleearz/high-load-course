package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
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

    private val parallelRequestsSemaphore = Semaphore(parallelPermits, true)

    private val rateLimiter = SlidingWindowRateLimiter(
        properties.rateLimitPerSec.toLong(),
        Duration.ofMillis(1000 + RATE_WINDOW_GUARD_MS),
    )

    private class PaymentAttempt(val latestStart: Long) {
        var submittedAt: Long? = null
    }

    private class RateWaitTimeout : IOException("Deadline exceeded while waiting for rate limit")

    private val client = OkHttpClient.Builder()
        .addNetworkInterceptor { chain ->
            val attempt = checkNotNull(chain.request().tag(PaymentAttempt::class.java))
            if (attempt.submittedAt != null) {
                processing.decrementAndGet()
                attempt.submittedAt = null
            }
            if (!awaitRatePermit(attempt.latestStart)) throw RateWaitTimeout()
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
            val transactionId = UUID.randomUUID()
            val processingMs = processingP99Ms
            val latestStart = deadline - processingMs - ADMISSION_MARGIN_MS
            val now = System.currentTimeMillis()

            if (now >= latestStart) {
                logger.debug("[$accountName] Payment $paymentId has insufficient time before deadline")
                reject(paymentId, transactionId, paymentStartedAt, "Insufficient time before deadline")
                return
            }

            val slotWaitMs = (latestStart - System.currentTimeMillis()).coerceAtLeast(0)
            val acquired = try {
                parallelRequestsSemaphore.tryAcquire(slotWaitMs, TimeUnit.MILLISECONDS)
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                false
            }
            if (!acquired) {
                logger.debug("[$accountName] Payment $paymentId failed to acquire parallel slot")
                reject(paymentId, transactionId, paymentStartedAt, "Failed to acquire parallel slot before deadline")
                return
            }
            permitHeld = true

            val attempt = PaymentAttempt(latestStart)
            var result = false
            var reason: String? = null
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
                }
            } catch (e: Exception) {
                if (e !is RateWaitTimeout) {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                }
                timedOut = e is InterruptedIOException
                reason = when (e) {
                    is RateWaitTimeout -> e.message
                    is SocketTimeoutException, is InterruptedIOException -> "HTTP request timeout after ${e.message}"
                    else -> e.message ?: "Unknown error"
                }
            } finally {
                val elapsed = attempt.submittedAt?.let { System.currentTimeMillis() - it }
                if (elapsed != null) {
                    recordProcessing(elapsed)
                    processing.decrementAndGet()
                }
                val releaseDelay = if (timedOut && elapsed != null) processingP99Ms - elapsed else 0L
                if (releaseDelay > 0) {
                    slotReleaser.schedule({ parallelRequestsSemaphore.release() }, releaseDelay, TimeUnit.MILLISECONDS)
                } else {
                    parallelRequestsSemaphore.release()
                }
                permitHeld = false
            }

            val processedAt = System.currentTimeMillis()
            if (!result) logger.warn("[$accountName] Payment $paymentId failed: $reason")
            val submittedAt = attempt.submittedAt ?: processedAt
            val waitTime = submittedAt - now
            paymentESService.update(paymentId) {
                it.logSubmission(success = attempt.submittedAt != null, transactionId, submittedAt, Duration.ofMillis(waitTime))
            }
            paymentESService.update(paymentId) {
                it.logProcessing(result, processedAt, transactionId, reason = reason)
            }
        } finally {
            if (permitHeld) {
                parallelRequestsSemaphore.release()
            }
            releaseAdmission()
        }
    }

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
        if (sorted.size < WARMUP_SAMPLES) {
            processingP99Ms = maxOf(p99, (expectedProcessingMs * WARMUP_FACTOR).toLong())
            processingAvgMs = processingP99Ms
        } else {
            processingP99Ms = p99
            processingAvgMs = sorted.average().toLong().coerceAtLeast(1)
        }
    }
}

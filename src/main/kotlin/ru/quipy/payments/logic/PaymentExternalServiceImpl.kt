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
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.ConcurrentLinkedDeque
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
        private const val ADMISSION_MARGIN_MS = 200L
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val expectedProcessingMs = properties.averageProcessingTime.toMillis()
    private val processingSamples = ConcurrentLinkedDeque<Long>()
    private val maxProcessingSamples = 512
    private val parallelPermits = properties.parallelRequests.coerceAtLeast(1)
    private val inFlight = AtomicInteger(0)

    private val parallelRequestsSemaphore = Semaphore(parallelPermits, true)

    private val rateLimiter = SlidingWindowRateLimiter(properties.rateLimitPerSec.toLong(), Duration.ofSeconds(1))

    private val client = OkHttpClient.Builder()
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
        val now = System.currentTimeMillis()
        val processingMs = processingP99Millis()
        val throughput = throughputPerSec(processingMs)
        while (true) {
            val current = inFlight.get()
            val predictedCompletion =
                now + (current / throughput) * 1000.0 + processingMs + ADMISSION_MARGIN_MS
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
            val processingMs = processingP99Millis()
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

            if (!awaitRatePermit(latestStart)) {
                logger.debug("[$accountName] Payment $paymentId missed rate limit window")
                reject(paymentId, transactionId, paymentStartedAt, "Deadline exceeded while waiting for rate limit")
                return
            }

            val afterWait = System.currentTimeMillis()
            val waitTime = afterWait - now
            val timeForProcessing = deadline - afterWait
            if (timeForProcessing < processingMs) {
                logger.debug(
                    "[$accountName] Payment $paymentId has only ${timeForProcessing}ms left, but need ${processingMs}ms"
                )
                reject(paymentId, transactionId, paymentStartedAt, "Insufficient time for processing after wait")
                return
            }

            logger.debug(
                "[$accountName] Payment $paymentId acquired slot after ${waitTime}ms, ${timeForProcessing}ms remaining"
            )

            try {
                paymentESService.update(paymentId) {
                    it.logSubmission(success = true, transactionId, afterWait, Duration.ofMillis(waitTime))
                }

                logger.debug("[$accountName] Submit: $paymentId , txId: $transactionId")

                val request = Request.Builder().run {
                    url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                    post(emptyBody)
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

                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, System.currentTimeMillis(), transactionId, reason = body.message)
                    }
                    recordProcessing(System.currentTimeMillis() - afterWait)
                }
            } catch (e: Exception) {
                logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                paymentESService.update(paymentId) {
                    it.logProcessing(
                        false,
                        System.currentTimeMillis(),
                        transactionId,
                        reason = when (e) {
                            is SocketTimeoutException -> "HTTP request timeout after ${e.message}"
                            else -> e.message ?: "Unknown error"
                        }
                    )
                }
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

    private fun awaitRatePermit(latestStart: Long): Boolean {
        while (!rateLimiter.tick()) {
            val remaining = latestStart - System.currentTimeMillis()
            if (remaining <= 0) {
                return false
            }
            try {
                Thread.sleep(minOf(5L, remaining))
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                return false
            }
        }
        return true
    }

    private fun reject(paymentId: UUID, transactionId: UUID, paymentStartedAt: Long, reason: String) {
        val now = System.currentTimeMillis()
        paymentESService.update(paymentId) {
            it.logSubmission(success = false, transactionId, now, Duration.ofMillis(now - paymentStartedAt))
            it.logProcessing(false, now, transactionId, reason = reason)
        }
    }

    private fun throughputPerSec(processingMs: Long): Double {
        val processingSec = processingMs.coerceAtLeast(1) / 1000.0
        val byParallel = parallelPermits / processingSec
        return minOf(properties.rateLimitPerSec.toDouble(), byParallel).coerceAtLeast(0.001)
    }

    private fun recordProcessing(durationMillis: Long) {
        processingSamples.addLast(durationMillis.coerceAtLeast(1))
        while (processingSamples.size > maxProcessingSamples) {
            processingSamples.pollFirst()
        }
    }

    private fun processingP99Millis(): Long {
        val values = processingSamples.toList()
        if (values.isEmpty()) {
            return expectedProcessingMs.coerceAtLeast(1)
        }
        val sorted = values.sorted()
        val index = ((sorted.size - 1) * 0.99).toInt()
        return sorted[index].coerceAtLeast(1)
    }
}

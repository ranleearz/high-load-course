package ru.quipy.common.utils

import java.time.Duration
import java.util.ArrayDeque

class SlidingWindowRateLimiter(
    private val rate: Long,
    window: Duration,
    private val nanoTime: () -> Long = System::nanoTime,
) : RateLimiter {
    private val windowNanos = window.toNanos()
    private val submissions = ArrayDeque<Long>()

    init {
        require(rate > 0)
        require(windowNanos > 0)
    }

    @Synchronized
    override fun tick(): Boolean {
        val now = nanoTime()
        removeExpired(now)
        if (submissions.size >= rate) return false
        submissions.addLast(now)
        return true
    }

    @Synchronized
    fun millisUntilAvailable(pendingRequests: Int): Long {
        require(pendingRequests > 0)
        val now = nanoTime()
        removeExpired(now)
        val free = rate - submissions.size
        val missing = pendingRequests - free
        if (missing <= 0) return 0

        val windowNumber = (missing - 1) / rate + 1
        val index = ((missing - 1) % rate).toInt()
        val submittedAt = submissions.elementAtOrNull(index) ?: now
        val waitNanos = (submittedAt - now + windowNumber * windowNanos).coerceAtLeast(0)
        return (waitNanos + 999_999) / 1_000_000
    }

    fun tickBlocking() {
        while (!tick()) Thread.sleep(10)
    }

    private fun removeExpired(now: Long) {
        while (submissions.isNotEmpty() && now - submissions.first >= windowNanos) {
            submissions.removeFirst()
        }
    }
}

package ru.quipy.common.utils

import java.time.Duration
import java.util.ArrayDeque
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class SlidingWindowRateLimiter(
    private val rate: Long,
    window: Duration,
    private val nanoTime: () -> Long = System::nanoTime,
) : RateLimiter {
    private val windowNanos = window.toNanos()
    private val submissions = ArrayDeque<Long>()
    private val waiters = ArrayDeque<Any>()
    private val lock = ReentrantLock(true)
    private val changed = lock.newCondition()

    init {
        require(rate > 0)
        require(windowNanos > 0)
    }

    override fun tick(): Boolean = lock.withLock {
        val now = nanoTime()
        removeExpired(now)
        if (waiters.isNotEmpty() || submissions.size >= rate) return@withLock false
        submissions.addLast(now)
        true
    }

    fun millisUntilAvailable(pendingRequests: Int): Long = lock.withLock {
        require(pendingRequests > 0)
        val now = nanoTime()
        removeExpired(now)
        val free = rate - submissions.size
        val missing = pendingRequests - free
        if (missing <= 0) return@withLock 0L

        val windowNumber = (missing - 1) / rate + 1
        val index = ((missing - 1) % rate).toInt()
        val submittedAt = submissions.elementAtOrNull(index) ?: now
        val waitNanos = (submittedAt - now + windowNumber * windowNanos).coerceAtLeast(0)
        (waitNanos + 999_999) / 1_000_000
    }

    fun awaitPermit(timeout: Duration): Boolean {
        val timeoutNanos = timeout.toNanos()
        require(timeoutNanos >= 0)
        if (timeoutNanos == 0L) return tick()
        val started = nanoTime()
        lock.lockInterruptibly()
        val ticket = Any()
        waiters.addLast(ticket)
        try {
            while (true) {
                val now = nanoTime()
                removeExpired(now)
                val remaining = timeoutNanos - (now - started)
                if (remaining <= 0) return false
                if (waiters.first === ticket && submissions.size < rate) {
                    waiters.removeFirst()
                    submissions.addLast(now)
                    changed.signalAll()
                    return true
                }
                val untilNextPermit = if (waiters.first === ticket && submissions.size >= rate) {
                    windowNanos - (now - submissions.first)
                } else remaining
                changed.awaitNanos(minOf(remaining, untilNextPermit).coerceAtLeast(1))
            }
        } finally {
            waiters.remove(ticket)
            changed.signalAll()
            lock.unlock()
        }
    }

    fun waitingCount(): Int = lock.withLock { waiters.size }

    fun tickBlocking() {
        awaitPermit(Duration.ofNanos(Long.MAX_VALUE))
    }

    private fun removeExpired(now: Long) {
        while (submissions.isNotEmpty() && now - submissions.first >= windowNanos) {
            submissions.removeFirst()
            changed.signalAll()
        }
    }
}

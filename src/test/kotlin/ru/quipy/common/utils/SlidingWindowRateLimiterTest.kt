package ru.quipy.common.utils

import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class SlidingWindowRateLimiterTest {
    private var now = 0L

    private fun advance(millis: Long) { now += millis * 1_000_000 }

    @Test
    fun `permits expire individually rather than at a common boundary`() {
        val limiter = SlidingWindowRateLimiter(2, Duration.ofSeconds(1)) { now }
        assertTrue(limiter.tick())
        advance(100)
        assertTrue(limiter.tick())
        assertFalse(limiter.tick())
        assertEquals(900L, limiter.millisUntilAvailable(1))
        assertEquals(1000L, limiter.millisUntilAvailable(2))
        advance(900)
        assertTrue(limiter.tick())
        assertFalse(limiter.tick())
        assertEquals(100L, limiter.millisUntilAvailable(1))
    }

    @Test
    fun `a burst can use free permits without waiting for a full window`() {
        val limiter = SlidingWindowRateLimiter(110, Duration.ofSeconds(1)) { now }
        assertEquals(0L, limiter.millisUntilAvailable(110))
        assertEquals(1000L, limiter.millisUntilAvailable(111))
        assertEquals(2000L, limiter.millisUntilAvailable(221))
    }

    @Test
    fun `forecast accounts for queued requests and does not reserve permits`() {
        val limiter = SlidingWindowRateLimiter(3, Duration.ofSeconds(1)) { now }
        assertTrue(limiter.tick())
        advance(100)
        assertTrue(limiter.tick())
        assertEquals(0L, limiter.millisUntilAvailable(1))
        assertEquals(900L, limiter.millisUntilAvailable(2))
        assertEquals(1000L, limiter.millisUntilAvailable(3))
        assertEquals(1000L, limiter.millisUntilAvailable(4))
        assertTrue(limiter.tick())
        assertFalse(limiter.tick())
    }

    @Test
    fun `concurrent callers cannot exceed a rolling window`() {
        val limiter = SlidingWindowRateLimiter(20, Duration.ofSeconds(1)) { 0L }
        val workers = Executors.newFixedThreadPool(8)
        try {
            val results = workers.invokeAll(List(200) { Callable { limiter.tick() } })
            assertEquals(20, results.count { it.get() })
        } finally {
            workers.shutdownNow()
        }
    }

    @Test
    fun `waiting callers keep their order and cannot be overtaken`() {
        val clock = AtomicLong(0)
        val limiter = SlidingWindowRateLimiter(1, Duration.ofSeconds(1), clock::get)
        val workers = Executors.newFixedThreadPool(2)
        try {
            assertTrue(limiter.tick())
            val first = workers.submit(Callable { limiter.awaitPermit(Duration.ofSeconds(5)) })
            awaitWaiting(limiter, 1)
            val second = workers.submit(Callable { limiter.awaitPermit(Duration.ofSeconds(5)) })
            awaitWaiting(limiter, 2)
            clock.set(1_000_000_000)
            assertFalse(limiter.tick())
            assertTrue(first.get(2, TimeUnit.SECONDS))
            assertFalse(second.isDone)
            clock.set(2_000_000_000)
            limiter.millisUntilAvailable(1)
            assertTrue(second.get(2, TimeUnit.SECONDS))
        } finally {
            workers.shutdownNow()
        }
    }

    @Test
    fun `timed out waiter releases its place`() {
        val limiter = SlidingWindowRateLimiter(1, Duration.ofMillis(100))
        assertTrue(limiter.tick())
        assertFalse(limiter.awaitPermit(Duration.ofMillis(1)))
        assertEquals(0, limiter.waitingCount())
        assertTrue(limiter.awaitPermit(Duration.ofSeconds(1)))
    }

    @Test
    fun `interrupted waiter releases its place`() {
        val limiter = SlidingWindowRateLimiter(1, Duration.ofSeconds(10))
        val worker = Executors.newSingleThreadExecutor()
        try {
            assertTrue(limiter.tick())
            val future = worker.submit(Callable { limiter.awaitPermit(Duration.ofSeconds(5)) })
            awaitWaiting(limiter, 1)
            future.cancel(true)
            awaitWaiting(limiter, 0)
            assertTrue(future.isCancelled)
        } finally {
            worker.shutdownNow()
        }
    }

    private fun awaitWaiting(limiter: SlidingWindowRateLimiter, count: Int) {
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2)
        while (limiter.waitingCount() != count && System.nanoTime() < deadline) Thread.sleep(1)
        assertEquals(count, limiter.waitingCount())
    }
}

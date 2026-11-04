package ru.quipy.common.utils

import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.Callable
import java.util.concurrent.Executors
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
}

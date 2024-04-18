package ru.quipy.payments.logic

import org.slf4j.LoggerFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.RateLimiter
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong

data class RequestData(
    val paymentId: UUID,
    val amount: Int,
    val paymentStartedAt: Long
)

class AccountQueue(
    private val window: OngoingWindow,
    private val rateLimiter: RateLimiter,
    private val accountName: String,
    private val callback: (paymentId: UUID, amount: Int, paymentStartedAt: Long) -> Unit
) {
    private val logger = LoggerFactory.getLogger(AccountQueue::class.java)
    private val executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 4)
    private val size = AtomicLong(0)

    fun enqueue(request: RequestData) {
        size.incrementAndGet()
        executor.submit {
            try {
                logger.warn("[$accountName] AccountQueue: submit ${request.paymentId}. Passed: ${now() - request.paymentStartedAt} ms. Window: ${window.availablePermits()}. Rate limiter: ${rateLimiter.availablePermits()}")
                window.acquire()
                rateLimiter.tickBlocking()

                logger.warn("[$accountName] AccountQueue: calling ${request.paymentId}. Passed: ${now() - request.paymentStartedAt} ms")
                callback(request.paymentId, request.amount, request.paymentStartedAt)
            }
            catch (e: Exception) {
                logger.error("[$accountName] AccountQueue: error - ${e.message}")
                window.release()
            }
        }
    }

    fun deque() {
        window.release()
        size.decrementAndGet()
    }

    fun getSize() = size.get()

}
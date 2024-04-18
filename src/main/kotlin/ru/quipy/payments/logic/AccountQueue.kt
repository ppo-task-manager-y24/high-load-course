package ru.quipy.payments.logic

import org.slf4j.LoggerFactory
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.RateLimiter
import java.util.*
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.Executors
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
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
    private val capacity: Int,
    private val callback: (paymentId: UUID, amount: Int, paymentStartedAt: Long) -> Unit,

) {
    private val logger = LoggerFactory.getLogger(AccountQueue::class.java)
//    private val executor = ThreadPoolExecutor(
//        Runtime.getRuntime().availableProcessors(), Runtime.getRuntime().availableProcessors() * 8, 10, TimeUnit.SECONDS,
//        ArrayBlockingQueue(10000),
//        NamedThreadFactory("queue-$accountName"),
//        ThreadPoolExecutor.DiscardOldestPolicy()
//    )
    private val executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 8)
    private val size = AtomicLong(0)
    private val queue = ArrayBlockingQueue<RequestData>(capacity)

    private fun processRequest()
    {
        executor.submit {
            try {
                logger.warn("[$accountName] AccountQueue-processRequest: wait")
                window.acquire()
                rateLimiter.tickBlocking()

                val request = queue.poll()

                logger.warn("[$accountName] AccountQueue-processRequest: calling ${request.paymentId}. Passed: ${now() - request.paymentStartedAt} ms")
                callback(request.paymentId, request.amount, request.paymentStartedAt)
            }
            catch (e: Exception) {
                logger.error("[$accountName] AccountQueue: error - ${e.message}")
                window.release()
            }
        }
    }

    fun tryEnqueue(request: RequestData, allowedNumReqBefore: Long) : Boolean {
        val s = size.incrementAndGet()
        if (s <= allowedNumReqBefore) {
            val result = queue.offer(request)
            logger.warn("[$accountName] AccountQueue-tryEnqueue: submit ${request.paymentId}. Passed: ${now() - request.paymentStartedAt} ms. result - $result")
            if (result)
                processRequest()
            return result
        }
        else {
            logger.warn("[$accountName] AccountQueue-tryEnqueue: submit ${request.paymentId}. Passed: ${now() - request.paymentStartedAt} ms. result - false")
            size.decrementAndGet()
            return false
        }
    }

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
        logger.warn("[$accountName] AccountQueue::deque window - ${window.availablePermits()}, size - ${size.get()}")
    }

    fun getSize() = size.get()

}
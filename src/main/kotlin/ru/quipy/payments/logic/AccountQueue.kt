package ru.quipy.payments.logic

import okhttp3.internal.notify
import okhttp3.internal.wait
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
    private val accountName: String,
    private val accountSpeed: Double,
    private val paymentOperationTimeoutSec: Long,
    private val requestAverageProcessingTimeMs: Long,
    private val rateLimiter: RateLimiter,
    private val window: OngoingWindow,
    private val callback: (paymentId: UUID, amount: Int, paymentStartedAt: Long) -> Unit
//    private val fallback:
) {
    private val executor = ThreadPoolExecutor(
        Runtime.getRuntime().availableProcessors(), Runtime.getRuntime().availableProcessors() * 16, 10, TimeUnit.SECONDS,
        ArrayBlockingQueue((paymentOperationTimeoutSec * accountSpeed).toInt() * 10),
        NamedThreadFactory("queue-$accountName"),
        ThreadPoolExecutor.DiscardOldestPolicy()
    )
    private val executor1 = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(), NamedThreadFactory("queue1-$accountName"))
    private val queue = ArrayBlockingQueue<RequestData>((paymentOperationTimeoutSec * accountSpeed).toInt())
    private val logger = LoggerFactory.getLogger(AccountQueue::class.java)

    fun tryEnqueue(request: RequestData): Boolean
    {
        logger.warn("[$accountName] tryEnqueue for payment ${request.paymentId}. Already passed: ${now() - request.paymentStartedAt} ms")
        val timeBeforeExpiration =
            paymentOperationTimeoutSec * 1000 - (now() - request.paymentStartedAt) - requestAverageProcessingTimeMs * 2
        val t = accountSpeed * timeBeforeExpiration / 1000


        var result = false
        var size = -1
        synchronized(queue) {
            size = queue.size
            if (size < t) {
                result = queue.offer(request)
            }
        }
        logger.warn("[$accountName] tryEnqueue result - $result,     size - $size,   ${paymentOperationTimeoutSec * accountSpeed},   t - $t")

        if (result)
        {
            executor.submit{
                processRequest()
            }
            return true
        }
        return false
    }

    private fun processRequest()
    {
        try {
            window.acquire()
            rateLimiter.tickBlocking()

            val request = queue.poll()

            executor1.submit {
                callback(request.paymentId, request.amount, request.paymentStartedAt)
            }
        }
        catch (e: Exception) {
            logger.error("AccountQueue - ${e.message}")
            window.release()
        }
    }
}

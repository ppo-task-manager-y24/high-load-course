package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.flow.callbackFlow
import okhttp3.*
import okhttp3.ResponseBody.Companion.toResponseBody
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.RateLimiter
import ru.quipy.common.utils.Summary
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.Executors
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.min

internal class RequestProcessingTimeInterceptor(
    private val requestAverageProcessingTime: Duration,
    private val paymentOperationTimeout: Duration
) : Interceptor {
    private val logger = LoggerFactory.getLogger(PaymentExternalServiceImpl::class.java)
    // calc percentile
    private val summary = Summary()
    private var minProcessingTime1: Long = Long.MAX_VALUE
    private var minProcessingTime2: Long = Long.MAX_VALUE
    private var lastUpdMin: Long = now()

    @Throws(IOException::class)
    override fun intercept(chain: Interceptor.Chain): Response {
        val request = chain.request()

        val paymentStartedAtHeader = request.header("paymentStartedAt")
        if (paymentStartedAtHeader != null) {
            val paymentStartedAt = paymentStartedAtHeader.toLong()
            val remainingTime = paymentOperationTimeout.toMillis() - (now() - paymentStartedAt)
            val minn = min(minProcessingTime1, minProcessingTime2)
//            val average = summary.getAverageMillis()
            if (minProcessingTime1 != Long.MAX_VALUE && minn > remainingTime) {
//            if (average != null && average.toMillis() * 0.1 > remainingTime) {
                logger.warn("RequestProcessingTimeInterceptor")
                return Response.Builder()
                    .code(418) // Whatever code
                    .body("".toResponseBody(null)) // Whatever body
                    .protocol(Protocol.HTTP_2)
                    .message("Dummy response")
                    .request(chain.request())
                    .build()
            }
        }

        val startProcessingAt = now()
        val response = chain.proceed(request)
        summary.reportExecution(now() - startProcessingAt)
        minProcessingTime2 = min(minProcessingTime2, now() - startProcessingAt)

        if (now() - lastUpdMin < 10000)
        {
            minProcessingTime1 = minProcessingTime2
            minProcessingTime2 = Long.MAX_VALUE
            lastUpdMin = now()
        }

        return response
    }

    fun GetAverage(): Long? {
        val average = summary.getAverageMillis()
        if (average != null)
            return average.toMillis()
        return null
    }
}

// Advice: always treat time as a Duration
class PaymentExternalServiceImpl(
    properties: ExternalServiceProperties
) : PaymentExternalService {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalServiceImpl::class.java)

        val paymentOperationTimeout = Duration.ofSeconds(80)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.request95thPercentileProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests
    private var speed: Double = min(parallelRequests.toDouble() / (requestAverageProcessingTime.toMillis().toDouble() / 1000), rateLimitPerSec.toDouble())
    private val requestProcessingTimeInterceptor = RequestProcessingTimeInterceptor(requestAverageProcessingTime, paymentOperationTimeout)
    private val responseProcessingThreadPool = ThreadPoolExecutor(
        Runtime.getRuntime().availableProcessors(), Runtime.getRuntime().availableProcessors(), 10, TimeUnit.SECONDS,
        ArrayBlockingQueue(10000),
        NamedThreadFactory("response-$accountName"),
        ThreadPoolExecutor.DiscardOldestPolicy()
    )
    private val window = OngoingWindow(parallelRequests)

    private var queue = AccountQueue(
        properties.accountName,
        GetSpeed(),
        paymentOperationTimeout.seconds,
        requestAverageProcessingTime.toMillis(),
        RateLimiter(rateLimitPerSec),
        window,
        ::paymentRequest
    )

    override fun GetSpeed() = speed

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    private lateinit var client: OkHttpClient

    init {
        val executor = ThreadPoolExecutor(
            Runtime.getRuntime().availableProcessors(), Runtime.getRuntime().availableProcessors() * 8, 10, TimeUnit.SECONDS,
            ArrayBlockingQueue(10000),
            ThreadPoolExecutor.DiscardOldestPolicy()
        )
        executor.setThreadFactory(NamedThreadFactory("httpclient-$accountName"))
        val dispatcher = Dispatcher(executor)
        dispatcher.maxRequests = 100000
        dispatcher.maxRequestsPerHost = 100000
        val connectionPool = ConnectionPool(parallelRequests, 5, TimeUnit.MINUTES)
        client = OkHttpClient.Builder().run {
            protocols(Collections.singletonList(Protocol.H2_PRIOR_KNOWLEDGE))
            connectionPool(connectionPool)
            dispatcher(dispatcher)
            addInterceptor(requestProcessingTimeInterceptor)
            build()
        }
    }

    private fun paymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId. Already passed: ${now() - paymentStartedAt} ms")

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId")
            header("paymentStartedAt", paymentStartedAt.toString())
            post(emptyBody)
        }.build()

        client.newCall(request).enqueue(object : Callback {
            override fun onFailure(call: Call, e: IOException) {
                window.release()
                responseProcessingThreadPool.submit {
                    when (e) {
                        is SocketTimeoutException -> {
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                            }
                        }

                        else -> {
                            logger.error(
                                "[$accountName] Payment failed for txId: $transactionId, payment: $paymentId",
                                e
                            )

                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = e.message)
                            }
                        }
                    }
                }
            }

            override fun onResponse(call: Call, response: Response) {
                window.release()
                responseProcessingThreadPool.submit {
                    response.use {
                        val body = try {
                            if (now() - paymentStartedAt > paymentOperationTimeout.toMillis()) {
                                logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId - timeout expired. ${now() - paymentStartedAt}")
                                ExternalSysResponse(false, "")
                            } else {
                                mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                            }
                        } catch (e: Exception) {
                            logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                            ExternalSysResponse(false, e.message)
                        }

                        logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                        // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                        // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                        paymentESService.update(paymentId) {
                            it.logProcessing(body.result, now(), transactionId, reason = body.message)
                        }
                    }
                }
            }
        })
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long): Boolean {
        return queue.tryEnqueue(RequestData(paymentId, amount, paymentStartedAt))
    }
}

public fun now() = System.currentTimeMillis()

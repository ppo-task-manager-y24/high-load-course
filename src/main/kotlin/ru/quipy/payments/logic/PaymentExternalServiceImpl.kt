package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.*
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.RateLimiter
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
import kotlin.math.min


// Advice: always treat time as a Duration
class PaymentExternalServiceImpl(
    private val properties: ExternalServiceProperties,
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

//    private val responseProcessingThreadPool = ThreadPoolExecutor(
//        Runtime.getRuntime().availableProcessors(), Runtime.getRuntime().availableProcessors() * 8, 10, TimeUnit.SECONDS,
//        ArrayBlockingQueue(10000),
//        NamedThreadFactory("response-$accountName"),
//        ThreadPoolExecutor.DiscardOldestPolicy()
//    )
    private val responseProcessingThreadPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 8)

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    private val window = OngoingWindow(parallelRequests)
    private val rateLimiter = RateLimiter(rateLimitPerSec)
    private val queue = AccountQueue(
        window,
        rateLimiter,
        accountName,
        ::paymentRequest
    )

    private lateinit var client: OkHttpClient

    init {
//        val executor = ThreadPoolExecutor(
//            Runtime.getRuntime().availableProcessors(), Runtime.getRuntime().availableProcessors() * 4, 10, TimeUnit.SECONDS,
//            ArrayBlockingQueue(10000),
//            NamedThreadFactory("httpclient-$accountName"),
//            ThreadPoolExecutor.DiscardOldestPolicy()
//        )
        val executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 4)
        val dispatcher = Dispatcher(executor)
        dispatcher.maxRequests = parallelRequests
        dispatcher.maxRequestsPerHost = parallelRequests
//        val connectionPool = ConnectionPool(parallelRequests, 5, TimeUnit.MINUTES)
        client = OkHttpClient.Builder().run {
            protocols(Collections.singletonList(Protocol.H2_PRIOR_KNOWLEDGE))
            dispatcher(dispatcher)
//            connectionPool(connectionPool)
            build()
        }
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long) : Boolean {
        val timeBeforeExpiration = paymentOperationTimeout.toMillis() - (now() - paymentStartedAt)
        val allowedNumReqBefore = (speed * (timeBeforeExpiration - requestAverageProcessingTime.toMillis()) / 1000).toLong()
        logger.warn("[$accountName] submitPaymentRequest: allowedNumReqBefore - $allowedNumReqBefore, size - ${queue.getSize()}. Passed: ${now() - paymentStartedAt} ms")
        if (allowedNumReqBefore <= queue.getSize())
            return false
        queue.enqueue(RequestData(paymentId, amount, paymentStartedAt))
        return true
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
            post(emptyBody)
        }.build()

        client.newCall(request).enqueue(object : Callback {
            override fun onFailure(call: Call, e: IOException) {
                queue.deque()
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
                queue.deque()
                responseProcessingThreadPool.submit {
                    response.use {
                        val body = try {
                            mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                            ExternalSysResponse(false, e.message)
                        }

                        logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, passed: ${now() - paymentStartedAt}, message: ${body.message}")

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
}

public fun now() = System.currentTimeMillis()
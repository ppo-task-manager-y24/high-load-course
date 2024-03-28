package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.*
import okhttp3.ResponseBody.Companion.toResponseBody
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.RateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit


internal class RateLimitInterceptor(
    private val rate: Int,
    private val timeUnit: TimeUnit = TimeUnit.SECONDS
) : Interceptor {
    private val rateLimiter: RateLimiter = RateLimiter(rate, timeUnit)

    @Throws(IOException::class)
    override fun intercept(chain: Interceptor.Chain): Response {
        val request = chain.request()
        rateLimiter.tickBlocking()
        val response = chain.proceed(request)

        return response
    }
}

internal class WindowControlInterceptor(
    maxWinSize: Int
) : Interceptor {
    private val window: OngoingWindow = OngoingWindow(maxWinSize)

    @Throws(IOException::class)
    override fun intercept(chain: Interceptor.Chain): Response {
        val request = chain.request()
        window.acquire()
        val response: Response
        try {
            response = chain.proceed(request)
        }
        finally {
            window.release()
        }

        return response
    }
}

internal class RequestProcessingTimeInterceptor(
    private val requestAverageProcessingTime: Duration,
    private val paymentOperationTimeout: Duration
) : Interceptor {
    private val logger = LoggerFactory.getLogger(PaymentExternalServiceImpl::class.java)

    @Throws(IOException::class)
    override fun intercept(chain: Interceptor.Chain): Response {
        val request = chain.request()

        val paymentStartedAtHeader = request.header("paymentStartedAt")
        if (paymentStartedAtHeader != null) {
            val paymentStartedAt = paymentStartedAtHeader.toLong()
            val remainingTime = paymentOperationTimeout.toMillis() - requestAverageProcessingTime.toMillis()
            if (now() - paymentStartedAt > remainingTime) {
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

        return chain.proceed(request)
    }
}

// Advice: always treat time as a Duration
class PaymentExternalServiceImpl(
    private val properties: ExternalServiceProperties,
) : PaymentExternalService {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalServiceImpl::class.java)
        val threadPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 4)

        val paymentOperationTimeout = Duration.ofSeconds(80)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.request95thPercentileProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests
    private val rateLimitInterceptor = RateLimitInterceptor(rateLimitPerSec)
    private val windowControlInterceptor = WindowControlInterceptor(parallelRequests)
    private val requestProcessingTimeInterceptor = RequestProcessingTimeInterceptor(requestAverageProcessingTime, paymentOperationTimeout)

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    private val httpClientExecutor = Executors.newSingleThreadExecutor()

    private val client = OkHttpClient.Builder().run {
        dispatcher(Dispatcher(httpClientExecutor))
        addInterceptor(requestProcessingTimeInterceptor)
        addNetworkInterceptor(rateLimitInterceptor)
        addNetworkInterceptor(windowControlInterceptor)
        build()
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long) {
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

//        threadPool.submit {
            client.newCall(request).enqueue(object : Callback {
                override fun onFailure(call: Call, e: IOException) {
                    when (e) {
                        is SocketTimeoutException -> {
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                            }
                        }

                        else -> {
                            logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = e.message)
                            }
                        }
                    }
                }

                override fun onResponse(call: Call, response: Response) {
                    response.use {
                        val body = try {
                            mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
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
            })
//        }



    }
}

public fun now() = System.currentTimeMillis()
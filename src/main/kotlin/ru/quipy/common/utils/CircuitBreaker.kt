import org.jetbrains.kotlin.com.google.common.collect.EvictingQueue
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.NamedThreadFactory
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

import kotlin.properties.Delegates.observable

interface CircuitBreakerInterface {
    fun canExecute(): Boolean
    fun execute(work: () -> Void)
    fun triggerSuccess()
    fun triggerFailure()
}

class CircuitBreaker(
    val name: String,
    private val failureRateThreshold: Double,
    private val slidingWindowSize: Int,
    private val resetTimeoutMs: Long,
    private val slidingWindowSizeWhenHalfOpen: Int
): CircuitBreakerInterface {
    enum class CircuitBreakerState {
        CLOSED, OPEN, HALF_OPEN
    }

    private var lastStateChangeTime = System.currentTimeMillis()
    private var state: CircuitBreakerState by observable(CircuitBreakerState.CLOSED) { _, oldValue, newValue ->
        if (newValue != oldValue) {
            onStateChange(newValue)
        }
    }
    private val executor = Executors.newSingleThreadScheduledExecutor(NamedThreadFactory(name))
    private val logger = LoggerFactory.getLogger(CircuitBreaker::class.java)
    private val window = EvictingQueue.create<Boolean>(slidingWindowSize)

    init {
        assert(slidingWindowSizeWhenHalfOpen <= slidingWindowSize)
        executor.scheduleAtFixedRate(
            { update() },
            0,
            5_000,
            TimeUnit.MILLISECONDS
        )
    }

    @Synchronized
    override fun canExecute() = state != CircuitBreakerState.OPEN

    @Synchronized
    override fun execute(work: () -> Void) {
        if (state == CircuitBreakerState.OPEN) throw CircuitBreakerOpenException("Circuit breaker is open")
        work()
    }

    @Synchronized
    override fun triggerFailure() {
        incrementFailureCount()
        when (state) {
            CircuitBreakerState.CLOSED -> {
                val rate = failureRate() ?: return
                if (rate >= failureRateThreshold) {
                    state = CircuitBreakerState.OPEN
                }
            }
            CircuitBreakerState.OPEN -> {}
            CircuitBreakerState.HALF_OPEN -> {
                onHalfOpen()
            }
        }
    }

    @Synchronized
    override fun triggerSuccess() {
        incrementCallCount()
        when (state) {
            CircuitBreakerState.CLOSED -> {}
            CircuitBreakerState.OPEN -> {}
            CircuitBreakerState.HALF_OPEN -> {
                onHalfOpen()
            }
        }
    }

    private fun onHalfOpen() {
        val rate = failureRate() ?: return
        state = if (rate < failureRateThreshold) {
            CircuitBreakerState.CLOSED
        } else {
            CircuitBreakerState.OPEN
        }
    }

    private fun incrementCallCount() {
        window.add(true)
    }

    private fun incrementFailureCount() {
        window.add(false)
    }

    private fun resetFailureCount() {
        window.clear()
    }

    private fun failureRate(): Double? {
        if (state == CircuitBreakerState.HALF_OPEN) {
            return failureRateImpl(slidingWindowSizeWhenHalfOpen)
        }

        return failureRateImpl(slidingWindowSize)
    }

    private fun failureRateImpl(minSize: Int): Double? {
        val calls = window.toArray()

        if (calls.count() < minSize) {
            return null
        }
        return calls.count { x -> x == true }.toDouble() / calls.size.toDouble()
    }

    private fun onStateChange(state: CircuitBreakerState) {
        lastStateChangeTime = System.currentTimeMillis()
        logger.error("[$name] now in state $state")
        if (state == CircuitBreakerState.HALF_OPEN) {
            resetFailureCount()
        }
    }

    @Synchronized
    private fun update() {
        if (state == CircuitBreakerState.OPEN && System.currentTimeMillis() - lastStateChangeTime >= resetTimeoutMs) {
            state = CircuitBreakerState.HALF_OPEN
        }
    }
}

class CircuitBreakerOpenException(message: String) : RuntimeException(message)
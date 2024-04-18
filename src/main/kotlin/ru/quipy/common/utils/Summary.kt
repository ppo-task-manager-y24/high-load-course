package ru.quipy.common.utils

import ru.quipy.payments.logic.now
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

class Summary(private val initial: Long, private val activeAfterNExecutions: Int = 30) {
    private val sum = AtomicLong(0)
    private val count = AtomicLong(0)

    private val resetPeriod = Duration.ofSeconds(10).toMillis()
    private var lastReset = now()
    private var n = 0L
    private val nums: PriorityQueue<Long> = PriorityQueue<Long>()
    private var count1 = 0L

    fun reportExecution(duration: Duration) {
        val v = duration.toMillis()
        sum.addAndGet(v)
        count.incrementAndGet()
        val passed = now() - lastReset
        synchronized(nums) {
            if (passed > resetPeriod) {
                count1 = 0
                nums.clear()
                n = 0
                lastReset = now()
            }
            n++
            val c = (n * 0.05).toLong()
            val peek = nums.peek()
            if (nums.size < c)
                nums.add(v)
            else if (peek != null && nums.peek() < v) {
                nums.remove()
                nums.add(v)
            } else {
            }
            count1++
        }
    }

    fun reportExecution(duration: Long) {
        val v = duration
        sum.addAndGet(v)
        count.incrementAndGet()
        val passed = now() - lastReset
        synchronized(nums) {
            if (passed > resetPeriod) {
                nums.clear()
                n = 0
                lastReset = now()
            }
            n++
            val c = (n * 0.05).toLong()
            val peek = nums.peek()
            if (nums.size < c)
                nums.add(v)
            else if (peek != null && peek < v) {
                nums.remove()
                nums.add(v)
            } else {
            }
        }
    }

    fun getAverage(): Double? {
        val c = count.get()
        if (c < activeAfterNExecutions) return null

        return sum.get().toDouble() / count.get()
    }

    fun getAverageMillis(): Duration? {
        val c = count.get()
        if (c < activeAfterNExecutions) return null

        return Duration.ofMillis(sum.get() / count.get())
    }

    fun get95thPercentile(): Long {
        var result = initial

        synchronized(nums) {
            if (count1 < activeAfterNExecutions) {
                result = initial
            }
            else {
                val a = nums.peek()
                result = if (a == null)
                    initial
                else
                    nums.peek()
            }
        }

        return result
    }
}
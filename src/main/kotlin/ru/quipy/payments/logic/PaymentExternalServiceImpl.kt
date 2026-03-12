package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.ratelimiter.RateLimiter
import io.github.resilience4j.ratelimiter.RateLimiterConfig
import kotlinx.coroutines.*
import kotlinx.coroutines.future.await
import kotlinx.coroutines.sync.Semaphore
import org.slf4j.LoggerFactory
import ru.quipy.config.EsWriterScope
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val esWriterScope: EsWriterScope,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_2)
        .build()

    private val semaphore = Semaphore(permits = parallelRequests)

    private val rateLimiter: RateLimiter = RateLimiter.of(
        "payments-$accountName",
        RateLimiterConfig.custom()
            .limitForPeriod(rateLimitPerSec)
            .limitRefreshPeriod(Duration.ofSeconds(1))
            .timeoutDuration(Duration.ofSeconds(5))
            .build()
    )

    private val circuitBreaker: CircuitBreaker = CircuitBreaker.of(
        "cb-$accountName",
        CircuitBreakerConfig.custom()
            .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
            .slidingWindowSize(20)
            .minimumNumberOfCalls(5)
            .failureRateThreshold(50f)
            .slowCallRateThreshold(80f)
            .slowCallDurationThreshold(Duration.ofMillis(500))
            .waitDurationInOpenState(Duration.ofSeconds(3))
            .permittedNumberOfCallsInHalfOpenState(5)
            .automaticTransitionFromOpenToHalfOpenEnabled(true)
            .build()
    )

    private val requestTimeout = 2000L
    private val maxRetries = 50
    private val retryDelayMs = 500L

    override suspend fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val transactionId = UUID.randomUUID()

        esWriterScope.esWriter.submit(paymentId) {
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }
        }

        var success = false
        var lastReason = "Max retries exceeded"

        for (attempt in 0 until maxRetries) {
            if (circuitBreaker.state == CircuitBreaker.State.OPEN) {
                logger.debug("[$accountName] CB OPEN, waiting before retry for $paymentId (attempt $attempt)")
                delay(retryDelayMs)
                continue
            }

            if (!circuitBreaker.tryAcquirePermission()) {
                delay(retryDelayMs)
                continue
            }

            val callStart = System.currentTimeMillis()
            try {
                val result = sendSingleRequest(transactionId, paymentId, amount)
                val callDuration = System.currentTimeMillis() - callStart

                if (result) {
                    circuitBreaker.onSuccess(callDuration, TimeUnit.MILLISECONDS)
                    success = true
                    break
                } else {
                    circuitBreaker.onError(callDuration, TimeUnit.MILLISECONDS, RuntimeException("Payment rejected by bank") as Throwable)
                    lastReason = "Payment rejected"
                    delay(retryDelayMs)
                }
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                val callDuration = System.currentTimeMillis() - callStart
                circuitBreaker.onError(callDuration, TimeUnit.MILLISECONDS, e as Throwable)
                lastReason = e.message ?: "Unknown error"
                logger.warn("[$accountName] Request failed for $paymentId (attempt $attempt): ${e.message}")
                delay(retryDelayMs)
            }
        }

        esWriterScope.esWriter.submit(paymentId) {
            paymentESService.update(paymentId) {
                it.logProcessing(success, now(), transactionId, reason = if (success) "OK" else lastReason)
            }
        }
    }

    private suspend fun sendSingleRequest(transactionId: UUID, paymentId: UUID, amount: Int): Boolean {
        semaphore.acquire()
        try {
            RateLimiter.waitForPermission(rateLimiter)

            val request = HttpRequest.newBuilder()
                .uri(URI("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"))
                .POST(HttpRequest.BodyPublishers.noBody())
                .timeout(Duration.ofMillis(requestTimeout))
                .build()

            val response = client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).await()

            val body = try {
                mapper.readValue(response.body(), ExternalSysResponse::class.java)
            } catch (e: Exception) {
                logger.error("[$accountName] [ERROR] txId: $transactionId, payment: $paymentId, code: ${response.statusCode()}")
                ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
            }

            return body.result
        } finally {
            semaphore.release()
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    override fun getAccountProperties() = properties
}

public fun now() = System.currentTimeMillis()

package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
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
import java.util.concurrent.atomic.AtomicBoolean

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
            .timeoutDuration(Duration.ofSeconds(2))
            .build()
    )

    private val hedgeDelayMs = 400L
    private val maxHedges = 5
    private val paymentTimeout = 1400L

    override suspend fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val transactionId = UUID.randomUUID()

        esWriterScope.esWriter.submit(paymentId) {
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }
        }

        val completed = AtomicBoolean(false)

        coroutineScope {
            val jobs = mutableListOf<Job>()

            repeat(maxHedges) { attempt ->
                if (attempt > 0) {
                    delay(hedgeDelayMs)
                }
                if (completed.get()) return@repeat

                val hedgeTxId = if (attempt == 0) transactionId else UUID.randomUUID()

                val job = launch {
                    try {
                        val result = sendSingleRequest(hedgeTxId, paymentId, amount)
                        if (completed.compareAndSet(false, true)) {
                            esWriterScope.esWriter.submit(paymentId) {
                                paymentESService.update(paymentId) {
                                    it.logProcessing(result, now(), hedgeTxId, reason = if (result) "OK" else "Failed")
                                }
                            }
                            jobs.forEach { j -> if (j != currentCoroutineContext()[Job]) j.cancel() }
                        }
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: Exception) {
                        logger.warn("[$accountName] Hedge attempt $attempt failed for $paymentId: ${e.message}")
                    }
                }
                jobs.add(job)
            }

            jobs.joinAll()

            if (!completed.get()) {
                esWriterScope.esWriter.submit(paymentId) {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "All hedge attempts failed")
                    }
                }
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
                .timeout(Duration.ofMillis(paymentTimeout))
                .build()

            val response = client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).await()

            val body = try {
                mapper.readValue(response.body(), ExternalSysResponse::class.java)
            } catch (e: Exception) {
                logger.error("[$accountName] [ERROR] txId: $transactionId, payment: $paymentId, code: ${response.statusCode()}, body: ${response.body()}")
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
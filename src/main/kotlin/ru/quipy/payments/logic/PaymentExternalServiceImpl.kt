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
            .timeoutDuration(Duration.ofSeconds(3))
            .build()
    )

    private val hedgeDelayMs = 150L
    private val maxHedges = 8
    private val requestTimeout = 1500L

    override suspend fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val transactionId = UUID.randomUUID()

        esWriterScope.esWriter.submit(paymentId) {
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }
        }

        val completed = AtomicBoolean(false)
        var success = false
        var winningTxId = transactionId

        coroutineScope {
            val hedgeLauncher = launch {
                repeat(maxHedges) { attempt ->
                    if (completed.get()) return@launch
                    if (attempt > 0) delay(hedgeDelayMs)
                    if (completed.get()) return@launch

                    val hedgeTxId = if (attempt == 0) transactionId else UUID.randomUUID()

                    launch {
                        try {
                            val result = sendSingleRequest(hedgeTxId, paymentId, amount)
                            if (result && completed.compareAndSet(false, true)) {
                                success = true
                                winningTxId = hedgeTxId
                                this@coroutineScope.coroutineContext[Job]?.cancelChildren()
                            }
                        } catch (_: CancellationException) {
                        } catch (e: Exception) {
                            logger.debug("[$accountName] Hedge $attempt failed for $paymentId: ${e.message}")
                        }
                    }
                }
            }

            hedgeLauncher.join()
        }

        esWriterScope.esWriter.submit(paymentId) {
            paymentESService.update(paymentId) {
                it.logProcessing(success, now(), winningTxId, reason = if (success) "OK" else "All hedges failed/timed out")
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
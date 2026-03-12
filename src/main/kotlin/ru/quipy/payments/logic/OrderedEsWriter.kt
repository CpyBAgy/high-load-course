package ru.quipy.payments.logic

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import java.util.*
import kotlin.math.abs

data class EsWrite(
    val key: UUID,
    val action: suspend () -> Unit
)

class OrderedEsWriter(
    scope: CoroutineScope,
    shards: Int = 400,
    queueSizePerShard: Int = 20000
) {
    companion object {
        val logger = LoggerFactory.getLogger(OrderedEsWriter::class.java)
    }

    private val channels = Array(shards) { Channel<EsWrite>(queueSizePerShard) }

    init {
        repeat(shards) { i ->
            scope.launch(Dispatchers.IO) {
                for (job in channels[i]) {
                    try {
                        job.action()
                    } catch (e: Exception) {
                        logger.error("[ERROR] Database sending error: ${e.message}")
                    }
                }
            }
        }
    }

    suspend fun submit(key: UUID, action: suspend () -> Unit) {
        val idx = shard(key)
        channels[idx].send(EsWrite(key, action))
    }

    private fun shard(key: UUID): Int {
        val h = key.mostSignificantBits xor key.leastSignificantBits
        return (abs(h.toInt()) % channels.size)
    }
}

package ru.quipy.config

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import org.springframework.stereotype.Component
import ru.quipy.payments.logic.OrderedEsWriter

@Component
class EsWriterScope {
    val coroutineScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    val esWriter: OrderedEsWriter = OrderedEsWriter(coroutineScope)
}

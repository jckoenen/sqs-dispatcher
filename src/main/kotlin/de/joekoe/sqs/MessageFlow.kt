package de.joekoe.sqs

import de.joekoe.sqs.flow.DrainControl
import kotlinx.coroutines.CoroutineScope
import org.slf4j.LoggerFactory

interface MessageFlow {
    fun subscribe(
        scope: CoroutineScope,
        queueName: Queue.Name,
        consumer: MessageConsumer<*>,
    ): DrainControl

    companion object {
        internal val logger = LoggerFactory.getLogger(MessageFlow::class.java)
    }
}

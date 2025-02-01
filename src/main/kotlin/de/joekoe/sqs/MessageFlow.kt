package de.joekoe.sqs

import de.joekoe.sqs.flow.DrainControl
import de.joekoe.sqs.flow.MessageAction
import de.joekoe.sqs.flow.MessageHandler
import kotlinx.coroutines.CoroutineScope
import org.slf4j.LoggerFactory

interface MessageFlow {
    fun subscribe(
        scope: CoroutineScope,
        queueName: Queue.Name,
        handler: MessageHandler<Message<String>, MessageAction>,
    ): DrainControl

    companion object {
        internal val logger = LoggerFactory.getLogger(MessageFlow::class.java)
    }
}

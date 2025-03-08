package de.joekoe.sqs

import de.joekoe.sqs.flow.DrainControl
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CoroutineScope
import org.slf4j.LoggerFactory

interface MessageFlow {
    fun subscribe(
        scope: CoroutineScope,
        queueName: Queue.Name,
        consumer: MessageConsumer,
        visibilityTimeout: Duration = 30.seconds,
    ): DrainControl

    companion object {
        internal val logger = LoggerFactory.getLogger(MessageFlow::class.java)
    }
}

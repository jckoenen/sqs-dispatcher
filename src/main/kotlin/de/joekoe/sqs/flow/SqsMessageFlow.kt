package de.joekoe.sqs.flow

import de.joekoe.sqs.MessageConsumer
import de.joekoe.sqs.MessageConsumer.Action.DeleteMessage
import de.joekoe.sqs.MessageConsumer.Action.MoveMessageToDlq
import de.joekoe.sqs.MessageFlow
import de.joekoe.sqs.OutboundMessage
import de.joekoe.sqs.Queue
import de.joekoe.sqs.SqsConnector
import de.joekoe.sqs.utils.TypedMap.Companion.byType
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map

internal class SqsMessageFlow(private val connector: SqsConnector) : MessageFlow {
    override fun subscribe(
        scope: CoroutineScope,
        queueName: Queue.Name,
        consumer: MessageConsumer<*>,
    ): DrainControl =
        flow {
                val queue = connector.getQueue(queueName) ?: TODO("error-handling")

                receiveFlow(queue, consumer, scope).collect(::emit)
            }
            .launchDraining(scope)

    private fun receiveFlow(
        queue: Queue,
        consumer: MessageConsumer<*>,
        scope: CoroutineScope,
    ): Flow<Unit> =
        drainSource()
            .map { connector.receiveMessages(queue) }
            .through(consumer.asStage().compose(VisibilityExtensionStage(connector, queue.visibilityTimeout, scope)))
            .map { actions ->
                val byType = actions.byType()

                byType
                    .get<DeleteMessage>()
                    .map(DeleteMessage::receiptHandle)
                    .let { connector.deleteMessages(queue.url, it) }
                    .plus(
                        byType
                            .get<MoveMessageToDlq>()
                            .map(MoveMessageToDlq::message)
                            .map(OutboundMessage.Companion::fromMessage)
                            .let { connector.sendMessages(queue.dlqUrl ?: TODO("error-handling"), it) })
                    .forEach {
                        MessageFlow.logger
                            .atWarn()
                            .addKeyValue("message.ref", it.reference)
                            .addKeyValue("message.action", DeleteMessage::class.simpleName)
                            .addKeyValue("failure.code", it.code)
                            .addKeyValue("failure.senderFault", it.senderFault)
                            .addKeyValue("failure.message", it.errorMessage)
                            .log("Message failed to be handled")
                    }
            }
}

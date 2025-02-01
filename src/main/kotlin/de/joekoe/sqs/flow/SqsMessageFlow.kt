package de.joekoe.sqs.flow

import de.joekoe.sqs.Message
import de.joekoe.sqs.MessageFlow
import de.joekoe.sqs.OutboundMessage
import de.joekoe.sqs.Queue
import de.joekoe.sqs.SqsConnector
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map

internal class SqsMessageFlow(private val connector: SqsConnector) : MessageFlow {
    override fun subscribe(
        scope: CoroutineScope,
        queueName: Queue.Name,
        handler: MessageHandler<Message<String>, MessageAction>,
    ): DrainControl =
        flow {
                val queue = connector.getQueue(queueName) ?: TODO("error-handling")

                receiveFlow(queue, handler).collect(::emit)
            }
            .launchDraining(scope)

    private fun receiveFlow(queue: Queue, handler: MessageHandler<Message<String>, MessageAction>) =
        drainSource()
            .map { connector.receiveMessages(queue) }
            .withAutomaticVisibilityExtension(connector, queue.visibilityTimeout, handler)
            .map { action ->
                // TODO: way better error handling needed
                val failed =
                    when (action) {
                        is MessageAction.RetryMessage -> {
                            emptyList()
                        }
                        is MessageAction.DeleteMessage -> {
                            connector.deleteMessages(queue.url, listOf(action.receiptHandle))
                        }
                        is MessageAction.MoveMessageToDlq -> {
                            connector.sendMessages(
                                queue.dlqUrl ?: TODO("error-handling"),
                                listOf(OutboundMessage.fromMessage(action.message)),
                            )
                            connector.deleteMessages(queue.url, listOf(action.receiptHandle))
                        }
                    }
                MessageFlow.logger
                    .atInfo()
                    .addKeyValue("message.ref", action.receiptHandle)
                    .addKeyValue("message.action", action::class.simpleName)
                    .addKeyValue("failures", failed)
                    .log("Message handled")
            }
}

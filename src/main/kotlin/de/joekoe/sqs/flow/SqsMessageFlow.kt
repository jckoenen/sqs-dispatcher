package de.joekoe.sqs.flow

import arrow.core.getOrElse
import de.joekoe.sqs.BatchResult
import de.joekoe.sqs.Failure
import de.joekoe.sqs.MessageConsumer
import de.joekoe.sqs.MessageConsumer.Action.DeleteMessage
import de.joekoe.sqs.MessageConsumer.Action.MoveMessageToDlq
import de.joekoe.sqs.MessageFlow
import de.joekoe.sqs.OutboundMessage
import de.joekoe.sqs.Queue
import de.joekoe.sqs.SqsConnector
import de.joekoe.sqs.allTags
import de.joekoe.sqs.utils.TypedMap.Companion.byType
import de.joekoe.sqs.utils.asTags
import de.joekoe.sqs.utils.id
import de.joekoe.sqs.utils.putAll
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
        flow { connector.getQueue(queueName).map { receiveFlow(it, consumer, scope).collect(::emit) } }
            .launchDraining(scope)

    private fun receiveFlow(
        queue: Queue,
        consumer: MessageConsumer<*>,
        scope: CoroutineScope,
    ): Flow<Unit> =
        drainSource()
            .map { connector.receiveMessages(queue).getOrElse { TODO("$it") } }
            .through(consumer.asStage().compose(VisibilityExtensionStage(connector, queue.visibilityTimeout, scope)))
            .map { actions ->
                val byType = actions.byType()

                // TODO: what to do with these
                moveToDlq(byType.get(), queue)
                delete(byType.get(), queue)
            }

    private suspend fun moveToDlq(toSend: List<MoveMessageToDlq>, queue: Queue) =
        toSend
            .map(MoveMessageToDlq::message) // formatting comment :(
            .map(OutboundMessage.Companion::fromMessage)
            .let { connector.sendMessages(queue.dlqUrl ?: TODO("error-handling"), it) }
            .also { logOutcome<MoveMessageToDlq>(it, queue) }

    private suspend fun delete(toDelete: List<DeleteMessage>, queue: Queue) =
        connector.deleteMessages(queue.url, toDelete.map(DeleteMessage::receiptHandle)).also {
            logOutcome<DeleteMessage>(it, queue)
        }

    private inline fun <reified A : MessageConsumer.Action> logOutcome(
        batchResult: BatchResult<Failure, *>,
        queue: Queue,
    ) {
        batchResult.getOrNull()?.let { success ->
            MessageFlow.logger
                .atDebug()
                .putAll(queue.id().asTags())
                .addKeyValue("messages.action", A::class.simpleName)
                .addKeyValue("messages.count", success.size)
                .log("Action succeeded")
        }

        batchResult.leftOrNull().orEmpty().forEach { (cause, messages) ->
            MessageFlow.logger
                .atWarn()
                .putAll(cause.allTags())
                .addKeyValue("messages.action", A::class.simpleName)
                .addKeyValue("messages.count", messages.size)
                .log("Action (partially?) failed")
        }
    }
}

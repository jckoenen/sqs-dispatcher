package de.joekoe.sqs.flow

import arrow.core.Nel
import arrow.core.leftIor
import de.joekoe.sqs.BatchResult
import de.joekoe.sqs.Failure
import de.joekoe.sqs.Message
import de.joekoe.sqs.MessageConsumer
import de.joekoe.sqs.MessageConsumer.Action.DeleteMessage
import de.joekoe.sqs.MessageConsumer.Action.MoveMessageToDlq
import de.joekoe.sqs.MessageConsumer.Action.RetryBackoff
import de.joekoe.sqs.OutboundMessage
import de.joekoe.sqs.Queue
import de.joekoe.sqs.SqsConnector
import de.joekoe.sqs.SqsFailure
import de.joekoe.sqs.allTags
import de.joekoe.sqs.impl.kotlin.SEND_OPERATION
import de.joekoe.sqs.impl.kotlin.batchCallFailed
import de.joekoe.sqs.impl.kotlin.combine
import de.joekoe.sqs.utils.TypedMap.Companion.byType
import de.joekoe.sqs.utils.id
import de.joekoe.sqs.utils.putAll
import de.joekoe.sqs.utils.resolveQueue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.onStart

fun SqsConnector.consume(
    queue: Queue,
    consumer: MessageConsumer,
    visibilityTimeout: Duration = 30.seconds,
): DrainableFlow<Nothing> = drainable {
    val stage = consumer.asStage().compose(VisibilityExtensionStage(this@consume, visibilityTimeout))

    consumeImpl(queue, stage, receive(queue, visibilityTimeout)).collect(::emit)
}

fun SqsConnector.consume(
    queueName: Queue.Name,
    consumer: MessageConsumer,
    visibilityTimeout: Duration = 30.seconds,
): DrainableFlow<Nothing> = drainable {
    val q = resolveQueue(queueName)

    consume(q, consumer, visibilityTimeout).collect(::emit)
}

@Suppress("UNCHECKED_CAST")
private fun SqsConnector.consumeImpl(
    queue: Queue,
    stage: FlowStage<Message<String>, MessageConsumer.Action>,
    messages: Flow<List<Message<String>>>,
): Flow<Nothing> =
    messages
        .through(stage)
        .map { actions ->
            val byType = actions.byType()

            byType.onMatching { moveToDlq(it, queue) }?.also(::logOutcome)
            byType.onMatching { delete(it, queue) }?.also(::logOutcome)
            byType.onMatching { backoff(it, queue) }?.also(::logOutcome)
        }
        .onStart { SqsConnector.logger.info("Consumer started") }
        .onCompletion { SqsConnector.logger.info("Consumer stopped") }
        .filter { false } as Flow<Nothing>

private suspend fun SqsConnector.moveToDlq(
    toSend: Nel<MoveMessageToDlq>,
    queue: Queue,
): BatchResult<SqsFailure.SendMessagesFailure, OutboundMessage> {
    val messages = toSend.map(MoveMessageToDlq::message).map(OutboundMessage::fromMessage)
    val dlq = queue.dlqUrl

    return if (dlq == null) {
        val failure =
            SqsFailure.QueueDoesNotExist(
                operation = SEND_OPERATION,
                queue = queue.id(),
                message = "The specified queue does not have a DLQ! Messages will be retried instead",
            )
        batchCallFailed(failure, messages, senderFault = true).leftIor()
    } else {
        sendMessages(dlq, messages)
    }
}

private suspend fun SqsConnector.backoff(toSend: Nel<RetryBackoff>, queue: Queue) =
    toSend
        .groupBy(RetryBackoff::backoffDuration, RetryBackoff::receiptHandle)
        .entries
        .asFlow()
        .map { (duration, handles) -> extendMessageVisibility(queue.url, handles, duration) }
        .combine()

private suspend fun SqsConnector.delete(toDelete: List<DeleteMessage>, queue: Queue) =
    deleteMessages(queue.url, toDelete.map(DeleteMessage::receiptHandle))

private fun logOutcome(batchResult: BatchResult<Failure, *>) {
    batchResult.getOrNull()?.let { success ->
        SqsConnector.logger.atDebug().addKeyValue("messages.count", success.size).log("Action succeeded")
    }

    batchResult.leftOrNull().orEmpty().forEach { (cause, messages) ->
        SqsConnector.logger
            .atWarn()
            .addKeyValue("messages.count", messages.size)
            .putAll(cause.allTags())
            .log("Action (partially?) failed")
    }
}

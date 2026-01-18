package io.github.jckoenen.flow

import arrow.core.Nel
import arrow.core.leftIor
import io.github.jckoenen.BatchResult
import io.github.jckoenen.Failure
import io.github.jckoenen.Message
import io.github.jckoenen.MessageConsumer
import io.github.jckoenen.MessageConsumer.Action.DeleteMessage
import io.github.jckoenen.MessageConsumer.Action.MoveMessageToDlq
import io.github.jckoenen.MessageConsumer.Action.RetryBackoff
import io.github.jckoenen.OutboundMessage
import io.github.jckoenen.Queue
import io.github.jckoenen.SqsConnector
import io.github.jckoenen.SqsFailure
import io.github.jckoenen.allTags
import io.github.jckoenen.impl.kotlin.SEND_OPERATION
import io.github.jckoenen.impl.kotlin.batchCallFailed
import io.github.jckoenen.impl.kotlin.combine
import io.github.jckoenen.utils.TypedMap.Companion.byType
import io.github.jckoenen.utils.id
import io.github.jckoenen.utils.putAll
import io.github.jckoenen.utils.resolveQueue
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

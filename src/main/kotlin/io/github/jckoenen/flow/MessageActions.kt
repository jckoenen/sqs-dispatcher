package io.github.jckoenen.flow

import arrow.core.Nel
import arrow.core.PotentiallyUnsafeNonEmptyOperation
import arrow.core.flatten
import arrow.core.leftIor
import arrow.core.toNonEmptyListOrThrow
import arrow.core.wrapAsNonEmptyListOrThrow
import io.github.jckoenen.BatchResult
import io.github.jckoenen.Failure
import io.github.jckoenen.Message
import io.github.jckoenen.MessageConsumer
import io.github.jckoenen.OutboundMessage
import io.github.jckoenen.Queue
import io.github.jckoenen.SqsConnector
import io.github.jckoenen.SqsFailure
import io.github.jckoenen.allTags
import io.github.jckoenen.impl.kotlin.SEND_OPERATION
import io.github.jckoenen.impl.kotlin.batchCallFailed
import io.github.jckoenen.impl.kotlin.reduce
import io.github.jckoenen.utils.TypedMap.Companion.byType
import io.github.jckoenen.utils.id
import io.github.jckoenen.utils.putAll
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.map

internal suspend fun SqsConnector.applyMessageActions(actions: List<MessageConsumer.Action>, queue: Queue) {
    val byType = actions.byType()

    byType.onMatching { moveToDlq(it, queue) }?.also(::logOutcome)
    byType.onMatching { delete(it, queue) }?.also(::logOutcome)
    byType.onMatching { backoff(it, queue) }?.also(::logOutcome)
}

private suspend fun SqsConnector.moveToDlq(
    toSend: Nel<MessageConsumer.Action.MoveMessageToDlq>,
    queue: Queue,
): BatchResult<SqsFailure, *> {
    val lookup =
        toSend.map(MessageConsumer.Action.MoveMessageToDlq::message).associateBy(OutboundMessage.Companion::fromMessage)
    val outbound = lookup.keys.toNonEmptyListOrThrow()
    val dlq = queue.dlq

    return if (dlq == null) {
        val failure =
            SqsFailure.QueueDoesNotExist(
                operation = SEND_OPERATION,
                queue = queue.id(),
                message = "The specified queue does not have a DLQ! Messages will be retried instead",
            )
        batchCallFailed(failure, outbound, senderFault = true).leftIor()
    } else {
        val sendResult = sendMessages(dlq.url, outbound)
        val outboundBySourceHandle = lookup.mapKeys { (_, value) -> value.receiptHandle }

        sendResult
            .mapLeft { it.mapKeys { (key, _) -> key as SqsFailure } } // up-cast to simplify merging below
            .map { sent -> sent.map(lookup::getValue).map(Message<*>::receiptHandle) }
            .map { toDelete ->
                deleteMessages(queue.url, toDelete)
                    .map { handles -> handles.map(outboundBySourceHandle::getValue) }
                    .mapLeft { failuresWithCause ->
                        failuresWithCause.mapValues { (_, failures) ->
                            failures.map { entry ->
                                SqsConnector.FailedBatchEntry(
                                    reference = outboundBySourceHandle.getValue(entry.reference),
                                    code = entry.code,
                                    errorMessage = entry.errorMessage,
                                    senderFault = entry.senderFault)
                            }
                        }
                    }
            }
            .flatten { l, r -> l + r }
    }
}

@OptIn(PotentiallyUnsafeNonEmptyOperation::class)
private suspend fun SqsConnector.backoff(toSend: Nel<MessageConsumer.Action.RetryBackoff>, queue: Queue) =
    toSend
        .groupBy(
            MessageConsumer.Action.RetryBackoff::backoffDuration, MessageConsumer.Action.RetryBackoff::receiptHandle)
        .mapValues { (_, handles) -> handles.wrapAsNonEmptyListOrThrow() } // groupBy guarantees being non-empty
        .entries
        .asFlow()
        .map { (duration, handles) -> extendMessageVisibility(queue.url, handles, duration) }
        .reduce()

private suspend fun SqsConnector.delete(toDelete: Nel<MessageConsumer.Action.DeleteMessage>, queue: Queue) =
    deleteMessages(queue.url, toDelete.map(MessageConsumer.Action.DeleteMessage::receiptHandle))

private fun logOutcome(batchResult: BatchResult<Failure, *>) {
    batchResult.getOrNull()?.let { success ->
        SqsConnector.Companion.logger.atDebug().addKeyValue("messages.count", success.size).log("Action succeeded")
    }

    batchResult.leftOrNull().orEmpty().forEach { (cause, messages) ->
        SqsConnector.Companion.logger
            .atWarn()
            .addKeyValue("messages.count", messages.size)
            .putAll(cause.allTags())
            .log("Action (partially?) failed")
    }
}

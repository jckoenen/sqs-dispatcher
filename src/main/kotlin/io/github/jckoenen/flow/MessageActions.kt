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
import io.github.jckoenen.MessageConsumer.Action
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
import io.github.jckoenen.impl.kotlin.reduce
import io.github.jckoenen.utils.TypedMap.Companion.byType
import io.github.jckoenen.utils.id
import io.github.jckoenen.utils.putAll
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.map

internal suspend fun SqsConnector.applyMessageActions(actions: List<Action>, queue: Queue) {
    val byType = actions.byType()

    byType.onMatching { moveToDlq(it, queue).logOutcome(it) }
    byType.onMatching { delete(it, queue).logOutcome(it) }
    byType.onMatching { backoff(it, queue).logOutcome(it) }
}

private suspend fun SqsConnector.moveToDlq(
    toSend: Nel<MoveMessageToDlq>,
    queue: Queue,
): BatchResult<SqsFailure, *> {
    val lookup = toSend.map(MoveMessageToDlq::message).associateBy(OutboundMessage.Companion::fromMessage)
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

        sendResult
            .mapLeft { it.mapKeys { (key, _) -> key as SqsFailure } } // up-cast to simplify merging below
            .map { sent -> sent.map(lookup::getValue).map(Message<*>::receiptHandle) }
            .map { toDelete -> deleteMessages(queue.url, toDelete) }
            .flatten { l, r -> l + r }
    }
}

@OptIn(PotentiallyUnsafeNonEmptyOperation::class)
private suspend fun SqsConnector.backoff(toSend: Nel<RetryBackoff>, queue: Queue) =
    toSend
        .groupBy(RetryBackoff::backoffDuration, RetryBackoff::receiptHandle)
        .mapValues { (_, handles) -> handles.wrapAsNonEmptyListOrThrow() } // groupBy guarantees being non-empty
        .entries
        .asFlow()
        .map { (duration, handles) -> extendMessageVisibility(queue.url, handles, duration) }
        .reduce()

private suspend fun SqsConnector.delete(toDelete: Nel<DeleteMessage>, queue: Queue) =
    deleteMessages(queue.url, toDelete.map(DeleteMessage::receiptHandle))

private inline fun <reified A : Action> BatchResult<Failure, *>.logOutcome(ignored: Nel<A>) {
    getOrNull()?.let { success ->
        SqsConnector.logger
            .atDebug()
            .addKeyValue("action", A::class.simpleName)
            .addKeyValue("messages.count", success.size)
            .log("Action succeeded")
    }

    leftOrNull().orEmpty().forEach { (cause, messages) ->
        SqsConnector.logger
            .atWarn()
            .addKeyValue("action", A::class.simpleName)
            .addKeyValue("messages.count", messages.size)
            .putAll(cause.allTags())
            .log("Action (partially?) failed")
    }
}

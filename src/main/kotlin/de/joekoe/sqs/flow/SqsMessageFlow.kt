package de.joekoe.sqs.flow

import arrow.core.Either
import arrow.core.Nel
import arrow.core.getOrElse
import arrow.core.leftIor
import arrow.resilience.Schedule
import arrow.resilience.retryEither
import de.joekoe.sqs.BatchResult
import de.joekoe.sqs.Failure
import de.joekoe.sqs.MessageConsumer
import de.joekoe.sqs.MessageConsumer.Action.DeleteMessage
import de.joekoe.sqs.MessageConsumer.Action.MoveMessageToDlq
import de.joekoe.sqs.MessageConsumer.Action.RetryBackoff
import de.joekoe.sqs.MessageFlow
import de.joekoe.sqs.MessageFlow.Companion.logger
import de.joekoe.sqs.OutboundMessage
import de.joekoe.sqs.Queue
import de.joekoe.sqs.SqsConnector
import de.joekoe.sqs.SqsFailure
import de.joekoe.sqs.allTags
import de.joekoe.sqs.impl.kotlin.SEND_OPERATION
import de.joekoe.sqs.impl.kotlin.batchCallFailed
import de.joekoe.sqs.impl.kotlin.combine
import de.joekoe.sqs.utils.TypedMap.Companion.byType
import de.joekoe.sqs.utils.asTags
import de.joekoe.sqs.utils.id
import de.joekoe.sqs.utils.putAll
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map

internal class SqsMessageFlow(private val connector: SqsConnector) : MessageFlow {

    override fun subscribe(
        scope: CoroutineScope,
        queueName: Queue.Name,
        consumer: MessageConsumer,
        visibilityTimeout: Duration,
    ): DrainControl =
        flow {
                val queue =
                    retryIndefinitely(10.seconds, 5.minutes) {
                        connector.getQueue(queueName).warnOnLeft("Could not resolve queue url. Retrying...")
                    }
                receiveFlow(queue, consumer, scope, visibilityTimeout).collect(::emit)
            }
            .launchDraining(scope)

    private fun receiveFlow(
        queue: Queue,
        consumer: MessageConsumer,
        scope: CoroutineScope,
        visibilityTimeout: Duration,
    ): Flow<Unit> =
        drainSource()
            .map {
                retryIndefinitely(1.seconds, 1.minutes) {
                    connector
                        .receiveMessages(queue, visibilityTimeout = visibilityTimeout)
                        .warnOnLeft("Failed to poll messages. Retrying...")
                }
            }
            .through(consumer.asStage().compose(VisibilityExtensionStage(connector, visibilityTimeout, scope)))
            .map { actions ->
                val byType = actions.byType()

                byType.onMatching { moveToDlq(it, queue) }?.also { logOutcome(it, queue) }
                byType.onMatching { delete(it, queue) }?.also { logOutcome(it, queue) }
                byType.onMatching { backoff(it, queue) }?.also { logOutcome(it, queue) }
            }

    private suspend fun moveToDlq(
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
            connector.sendMessages(dlq, messages)
        }
    }

    private suspend fun backoff(toSend: Nel<RetryBackoff>, queue: Queue) =
        toSend
            .groupBy(RetryBackoff::backoffDuration, RetryBackoff::receiptHandle)
            .entries
            .asFlow()
            .map { (duration, handles) -> connector.extendMessageVisibility(queue.url, handles, duration) }
            .combine()

    private suspend fun delete(toDelete: List<DeleteMessage>, queue: Queue) =
        connector.deleteMessages(queue.url, toDelete.map(DeleteMessage::receiptHandle))

    private fun logOutcome(batchResult: BatchResult<Failure, *>, queue: Queue) {
        batchResult.getOrNull()?.let { success ->
            logger
                .atDebug()
                .putAll(queue.id().asTags())
                .addKeyValue("messages.count", success.size)
                .log("Action succeeded")
        }

        batchResult.leftOrNull().orEmpty().forEach { (cause, messages) ->
            logger
                .atWarn()
                .putAll(cause.allTags())
                .addKeyValue("messages.count", messages.size)
                .log("Action (partially?) failed")
        }
    }

    private fun <F : Failure, T> Either<F, T>.warnOnLeft(message: String) = onLeft {
        logger.atWarn().putAll(it.allTags()).log(message)
    }

    private suspend inline fun <T> retryIndefinitely(
        base: Duration,
        max: Duration,
        f: () -> Either<Failure, T>,
    ): T =
        Schedule.exponential<Any>(base)
            .doUntil { _, duration -> duration < max }
            .andThen(Schedule.spaced<Any>(max) and Schedule.forever())
            .jittered(min = 0.5, max = 1.5)
            .retryEither(f)
            .getOrElse { error("Indefinite retry exhausted!") }
}

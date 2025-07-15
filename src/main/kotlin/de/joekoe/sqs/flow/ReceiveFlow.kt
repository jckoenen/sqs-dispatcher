package de.joekoe.sqs.flow

import arrow.core.Either
import arrow.core.Nel
import arrow.core.toNonEmptyListOrNull
import de.joekoe.sqs.Failure
import de.joekoe.sqs.Message
import de.joekoe.sqs.Queue
import de.joekoe.sqs.SqsConnector
import de.joekoe.sqs.allTags
import de.joekoe.sqs.utils.asTags
import de.joekoe.sqs.utils.id
import de.joekoe.sqs.utils.mdc
import de.joekoe.sqs.utils.putAll
import de.joekoe.sqs.utils.resolveQueue
import de.joekoe.sqs.utils.retryIndefinitely
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.mapNotNull
import kotlinx.coroutines.flow.onEach

/**
 * Continuously receives messages from the specified SQS queue as a stream. The method uses indefinite retry logic to
 * handle transient polling errors and processes incoming messages as a drainable flow.
 *
 * @param queue The SQS queue from which messages are to be received.
 * @param visibilityTimeout The duration for which a polled message is invisible to other clients.
 * @return A drainable flow emitting lists of messages containing the content as strings.
 * @see DrainableFlow
 */
fun SqsConnector.receive(
    queue: Queue,
    visibilityTimeout: Duration = 30.seconds,
): DrainableFlow<Nel<Message<String>>> =
    drainSource()
        .map {
            retryIndefinitely(1.seconds, 1.minutes) {
                receiveMessages(queue, visibilityTimeout = visibilityTimeout)
                    .warnOnLeft("Failed to poll messages. Retrying…")
            }
        }
        .onEach { if (it.isEmpty()) SqsConnector.logger.debug("No messages received") }
        .mapNotNull { it.toNonEmptyListOrNull() }
        .flowOn(mdc(queue.id().asTags()))
        .drainable()

/**
 * Receives messages from an SQS queue as a drainable flow, allowing for continuous or controlled consumption of
 * messages. The method retries indefinitely to fetch the queue details if the queue is not available or fails to
 * resolve initially. It then collects messages from the queue while respecting the specified visibility timeout.
 *
 * @param queueName The name of the SQS queue from which messages are to be received.
 * @param visibilityTimeout The duration a retrieved message should remain invisible to other consumers. Defaults to 30
 *   seconds.
 * @return A drainable flow emitting lists of messages containing the content as strings.
 */
fun SqsConnector.receive(
    queueName: Queue.Name,
    visibilityTimeout: Duration = 30.seconds,
): DrainableFlow<Nel<Message<String>>> =
    channelFlow {
            val queue = resolveQueue(queueName)
            receive(queue, visibilityTimeout).collect(::send)
        }
        .drainable()

private fun <F : Failure, T> Either<F, T>.warnOnLeft(message: String) = onLeft {
    SqsConnector.logger.atWarn().putAll(it.allTags()).log(message)
}

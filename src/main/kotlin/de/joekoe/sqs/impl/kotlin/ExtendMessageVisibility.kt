package de.joekoe.sqs.impl.kotlin

import arrow.core.Nel
import arrow.core.leftIor
import arrow.core.toNonEmptyListOrNull
import arrow.core.unzip
import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.changeMessageVisibilityBatch
import aws.sdk.kotlin.services.sqs.model.ChangeMessageVisibilityBatchRequestEntry
import de.joekoe.sqs.BatchResult
import de.joekoe.sqs.FailuresWithCause
import de.joekoe.sqs.Message
import de.joekoe.sqs.Queue
import de.joekoe.sqs.SqsConnector
import de.joekoe.sqs.SqsFailure
import de.joekoe.sqs.SqsFailure.ChangeMessagesFailure
import kotlin.time.Duration
import kotlinx.coroutines.flow.map

internal const val CHANGE_OPERATION = "SQS.ChangeMessageVisibilities"

internal suspend fun SqsClient.extendMessageVisibility(
    queueUrl: Queue.Url,
    messages: Collection<Message.ReceiptHandle>,
    duration: Duration,
): BatchResult<ChangeMessagesFailure, Message.ReceiptHandle> =
    messages
        .chunkForBatching { i, handle ->
            ChangeMessageVisibilityBatchRequestEntry {
                id = i.toString()
                receiptHandle = handle.value
                visibilityTimeout = duration.inWholeSeconds.toInt()
            }
        }
        .map { chunk ->
            val (inChunk, batch) = chunk.unzip()

            doChange(queueUrl, batch, inChunk)
        }
        .combine()

private suspend fun SqsClient.doChange(
    queueUrl: Queue.Url,
    batch: Nel<ChangeMessageVisibilityBatchRequestEntry>,
    inChunk: Nel<Message.ReceiptHandle>,
): BatchResult<ChangeMessagesFailure, Message.ReceiptHandle> =
    execute<ChangeMessagesFailure, _>(convertCommonExceptions(queueUrl.leftIor(), CHANGE_OPERATION)) {
            changeMessageVisibilityBatch {
                this.queueUrl = queueUrl.value
                entries = batch
            }
        }
        .mapLeft { batchCallFailed(it, inChunk) }
        .fold(
            ifLeft = { it.leftIor() },
            ifRight = {
                splitFailureAndSuccess(CHANGE_OPERATION, queueUrl.leftIor(), inChunk, it.failed)
                    .mapLeft(::extractAlreadyDeleted)
            },
        )

private fun <T : Any> extractAlreadyDeleted(
    map: FailuresWithCause<SqsFailure.PartialFailure, T>
): FailuresWithCause<ChangeMessagesFailure, T> {
    val (k, failures) = map.entries.single() // TODO: generalize
    val (alreadyDeleted, others) = failures.partition(::isAlreadyDeleted)

    return buildMap {
        others.toNonEmptyListOrNull()?.let { put(k, it) }
        alreadyDeleted.toNonEmptyListOrNull()?.let { put(ChangeMessagesFailure.MessageAlreadyDeleted(k.queue), it) }
    }
}

private fun isAlreadyDeleted(entry: SqsConnector.FailedBatchEntry<*>) =
    entry.senderFault == false && entry.code == "InvalidParameterValueException"

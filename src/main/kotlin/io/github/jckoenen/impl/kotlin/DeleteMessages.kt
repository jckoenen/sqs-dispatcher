package io.github.jckoenen.impl.kotlin

import arrow.core.Nel
import arrow.core.leftIor
import arrow.core.unzip
import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.deleteMessageBatch
import aws.sdk.kotlin.services.sqs.model.DeleteMessageBatchRequestEntry
import io.github.jckoenen.BatchResult
import io.github.jckoenen.Message
import io.github.jckoenen.Queue
import io.github.jckoenen.SqsFailure.DeleteMessagesFailure
import kotlinx.coroutines.flow.map

private const val DELETE_OPERATION = "SQS.DeleteMessages"

internal suspend fun SqsClient.deleteMessages(
    queueUrl: Queue.Url,
    handles: Collection<Message.ReceiptHandle>,
): BatchResult<DeleteMessagesFailure, Message.ReceiptHandle> =
    handles
        .chunkForBatching { i, handle ->
            DeleteMessageBatchRequestEntry {
                id = i.toString()
                receiptHandle = handle.value
            }
        }
        .map { chunk ->
            val (inChunk, requestEntries) = chunk.unzip()
            doDelete(queueUrl, requestEntries, inChunk)
        }
        .combine()

private suspend fun SqsClient.doDelete(
    queueUrl: Queue.Url,
    batch: Nel<DeleteMessageBatchRequestEntry>,
    inChunk: Nel<Message.ReceiptHandle>,
): BatchResult<DeleteMessagesFailure, Message.ReceiptHandle> =
    execute<DeleteMessagesFailure, _>(convertCommonExceptions(queueUrl.leftIor(), DELETE_OPERATION)) {
            deleteMessageBatch {
                this.queueUrl = queueUrl.value
                entries = batch
            }
        }
        .mapLeft { batchCallFailed(it, inChunk) }
        .fold(
            ifLeft = { it.leftIor() },
            ifRight = { splitFailureAndSuccess(DELETE_OPERATION, queueUrl.leftIor(), inChunk, it.failed) },
        )

package de.joekoe.sqs.impl.kotlin

import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.deleteMessageBatch
import aws.sdk.kotlin.services.sqs.model.DeleteMessageBatchRequestEntry
import de.joekoe.sqs.Message
import de.joekoe.sqs.Queue
import de.joekoe.sqs.SqsConnector
import kotlinx.coroutines.flow.map

internal suspend fun SqsClient.deleteMessages(
    queueUrl: Queue.Url,
    handles: Collection<Message.ReceiptHandle>,
): List<SqsConnector.FailedBatchEntry<Message.ReceiptHandle>> =
    handles
        .chunkForBatching { i, handle ->
            DeleteMessageBatchRequestEntry {
                id = i.toString()
                receiptHandle = handle.value
            }
        }
        .map { chunk ->
            val (inChunk, requestEntries) = chunk.unzip()
            val response = deleteMessageBatch {
                this.queueUrl = queueUrl.value
                entries = requestEntries
            }
            response.failed.map {
                SqsConnector.FailedBatchEntry(
                    reference = inChunk[it.id.toInt()],
                    code = it.code,
                    errorMessage = it.message,
                    senderFault = it.senderFault,
                )
            }
        }
        .flattenToList()

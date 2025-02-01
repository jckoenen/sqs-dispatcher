package de.joekoe.sqs.impl.kotlin

import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.changeMessageVisibilityBatch
import aws.sdk.kotlin.services.sqs.model.ChangeMessageVisibilityBatchRequestEntry
import de.joekoe.sqs.Message
import de.joekoe.sqs.Queue
import de.joekoe.sqs.SqsConnector
import kotlin.time.Duration
import kotlinx.coroutines.flow.map

internal suspend fun SqsClient.extendMessageVisibility(
    queueUrl: Queue.Url,
    messages: Collection<Message.ReceiptHandle>,
    duration: Duration,
): List<SqsConnector.FailedBatchEntry<Message.ReceiptHandle>> =
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
            val response = changeMessageVisibilityBatch {
                this.queueUrl = queueUrl.value
                entries = batch
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

package de.joekoe.sqs.impl.kotlin

import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.changeMessageVisibilityBatch
import aws.sdk.kotlin.services.sqs.model.ChangeMessageVisibilityBatchRequestEntry
import de.joekoe.sqs.Message
import de.joekoe.sqs.Queue
import de.joekoe.sqs.SqsConnector
import kotlin.time.Duration
import kotlinx.coroutines.flow.map

internal suspend fun <T : Any> SqsClient.extendMessageVisibility(
    queue: Queue,
    messages: List<Message<T>>,
    duration: Duration,
): List<SqsConnector.FailedBatchEntry<T>> =
    messages
        .chunkForBatching {
            ChangeMessageVisibilityBatchRequestEntry {
                id = it.id.value
                receiptHandle = it.receiptHandle.value
                visibilityTimeout = duration.inWholeSeconds.toInt()
            }
        }
        .map { (messages, batch) ->
            val response = changeMessageVisibilityBatch {
                queueUrl = queue.url.value
                entries = batch
            }

            response.failed.map {
                SqsConnector.FailedBatchEntry(
                    message = messages.getValue(Message.Id(it.id)),
                    code = it.code,
                    errorMessage = it.message,
                    senderFault = it.senderFault,
                )
            }
        }
        .flattenToList()

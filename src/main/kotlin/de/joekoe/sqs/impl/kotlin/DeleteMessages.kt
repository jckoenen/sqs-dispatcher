package de.joekoe.sqs.impl.kotlin

import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.deleteMessageBatch
import aws.sdk.kotlin.services.sqs.model.DeleteMessageBatchRequestEntry
import de.joekoe.sqs.Message
import de.joekoe.sqs.Queue
import de.joekoe.sqs.SqsConnector
import kotlinx.coroutines.flow.map

internal suspend fun <T : Any> SqsClient.deleteMessages(
    queue: Queue,
    messages: List<Message<T>>,
): List<SqsConnector.FailedBatchEntry<T>> =
    messages
        .chunkForBatching { message ->
            DeleteMessageBatchRequestEntry {
                id = message.id.value
                receiptHandle = message.receiptHandle.value
            }
        }
        .map { (messages, batch) ->
            val response = deleteMessageBatch {
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

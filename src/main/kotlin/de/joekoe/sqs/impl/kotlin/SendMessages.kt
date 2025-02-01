package de.joekoe.sqs.impl.kotlin

import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.model.MessageAttributeValue
import aws.sdk.kotlin.services.sqs.model.SendMessageBatchRequestEntry
import aws.sdk.kotlin.services.sqs.sendMessageBatch
import com.fasterxml.jackson.databind.ObjectMapper
import de.joekoe.sqs.OutboundMessage
import de.joekoe.sqs.Queue
import de.joekoe.sqs.SqsConnector
import kotlinx.coroutines.flow.map

internal suspend fun <T : Any> SqsClient.sendMessages(
    queueUrl: Queue.Url,
    json: ObjectMapper,
    messages: Collection<OutboundMessage<T>>,
): List<SqsConnector.FailedBatchEntry<OutboundMessage<T>>> =
    messages
        .chunkForBatching { i, msg ->
            SendMessageBatchRequestEntry {
                id = i.toString()
                messageAttributes = msg.attributes.mapValues { (_, v) -> MessageAttributeValue { stringValue = v } }
                messageBody = json.writeValueAsString(msg.content)
                messageDeduplicationId = msg.fifo?.deduplicationId?.value
                messageGroupId = msg.fifo?.groupId?.value
            }
        }
        .map { chunk ->
            val (inChunk, batch) = chunk.unzip()

            val response = sendMessageBatch {
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

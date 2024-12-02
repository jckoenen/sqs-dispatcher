package de.joekoe.sqs.impl.kotlin

import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.model.SendMessageBatchRequestEntry
import aws.sdk.kotlin.services.sqs.sendMessageBatch
import com.fasterxml.jackson.databind.ObjectMapper
import de.joekoe.sqs.Message
import de.joekoe.sqs.Queue
import de.joekoe.sqs.SqsConnector
import de.joekoe.sqs.map
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.fold
import kotlinx.coroutines.flow.map

internal suspend fun SqsClient.sendMessages(
    json: ObjectMapper,
    queue: Queue,
    messages: List<Message<*>>,
): List<SqsConnector.SendFailure<*>> =
    messages
        .chunked(10) { chunk -> chunk.map { msg -> msg.map(json::writeValueAsString) } }
        .asFlow()
        .map { chunk ->
            val response = sendMessageBatch {
                queueUrl = queue.url.value
                entries =
                    chunk.map { msg ->
                        SendMessageBatchRequestEntry {
                            id = msg.id.value
                            messageBody = msg.content
                            messageDeduplicationId = (msg as? Message.Fifo<*>)?.deduplicationId?.value
                            messageGroupId = (msg as? Message.Fifo<*>)?.groupId?.value
                        }
                    }
            }
            if (response.failed.isNotEmpty()) {
                val byId = chunk.associateBy(Message<*>::id)
                response.failed.map {
                    SqsConnector.SendFailure(
                        byId.getValue(Message.Id(it.id)),
                        it.code,
                        it.message,
                        it.senderFault,
                    )
                }
            } else {
                emptyList()
            }
        }
        .fold(emptyList()) { acc, value -> acc + value }

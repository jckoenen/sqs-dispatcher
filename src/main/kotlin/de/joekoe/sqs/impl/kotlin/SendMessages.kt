package de.joekoe.sqs.impl.kotlin

import arrow.core.Nel
import arrow.core.leftIor
import arrow.core.unzip
import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.model.MessageAttributeValue
import aws.sdk.kotlin.services.sqs.model.SendMessageBatchRequestEntry
import aws.sdk.kotlin.services.sqs.sendMessageBatch
import com.fasterxml.jackson.databind.ObjectMapper
import de.joekoe.sqs.BatchResult
import de.joekoe.sqs.OutboundMessage
import de.joekoe.sqs.Queue
import de.joekoe.sqs.SqsFailure.SendMessagesFailure
import kotlinx.coroutines.flow.map

private const val SEND_OPERATION = "SQS.SendMessages"

internal suspend fun <T : Any> SqsClient.sendMessages(
    queueUrl: Queue.Url,
    json: ObjectMapper,
    messages: Collection<OutboundMessage<T>>, // TODO: get rid of T here, this should be string only
): BatchResult<SendMessagesFailure, OutboundMessage<T>> =
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

            doSend(queueUrl, batch, inChunk)
        }
        .combine()

private suspend fun <T : Any> SqsClient.doSend(
    queueUrl: Queue.Url,
    batch: Nel<SendMessageBatchRequestEntry>,
    inChunk: Nel<OutboundMessage<T>>,
): BatchResult<SendMessagesFailure, OutboundMessage<T>> =
    execute<SendMessagesFailure, _>(convertCommonExceptions(queueUrl.leftIor(), SEND_OPERATION)) {
            sendMessageBatch {
                this.queueUrl = queueUrl.value
                entries = batch
            }
        }
        .mapLeft { batchCallFailed(it, inChunk) }
        .fold(
            ifLeft = { it.leftIor() },
            ifRight = { splitFailureAndSuccess(SEND_OPERATION, queueUrl.leftIor(), inChunk, it.failed) },
        )

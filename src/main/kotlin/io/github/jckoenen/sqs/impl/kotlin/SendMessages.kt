package io.github.jckoenen.sqs.impl.kotlin

import arrow.core.Nel
import arrow.core.NonEmptyCollection
import arrow.core.leftIor
import arrow.core.unzip
import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.model.MessageAttributeValue
import aws.sdk.kotlin.services.sqs.model.SendMessageBatchRequestEntry
import aws.sdk.kotlin.services.sqs.sendMessageBatch
import io.github.jckoenen.sqs.BatchResult
import io.github.jckoenen.sqs.OutboundMessage
import io.github.jckoenen.sqs.Queue
import io.github.jckoenen.sqs.SqsFailure.SendMessagesFailure
import kotlinx.coroutines.flow.map

internal const val SEND_OPERATION = "SQS.SendMessages"

internal suspend fun SqsClient.sendMessages(
    queueUrl: Queue.Url,
    messages: NonEmptyCollection<OutboundMessage>,
): BatchResult<SendMessagesFailure, OutboundMessage> =
    messages
        .chunkForBatching { i, msg ->
            SendMessageBatchRequestEntry {
                id = i.toString()
                messageAttributes =
                    msg.attributes.mapValues { (_, v) ->
                        MessageAttributeValue {
                            dataType = "String"
                            stringValue = v
                        }
                    }
                messageBody = msg.content
                messageDeduplicationId = msg.fifo?.deduplicationId?.value
                messageGroupId = msg.fifo?.groupId?.value
            }
        }
        .map { chunk ->
            val (inChunk, batch) = chunk.unzip()

            doSend(queueUrl, batch, inChunk)
        }
        .reduce()

private suspend fun SqsClient.doSend(
    queueUrl: Queue.Url,
    batch: Nel<SendMessageBatchRequestEntry>,
    inChunk: Nel<OutboundMessage>,
): BatchResult<SendMessagesFailure, OutboundMessage> =
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

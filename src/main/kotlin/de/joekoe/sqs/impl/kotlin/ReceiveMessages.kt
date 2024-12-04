package de.joekoe.sqs.impl.kotlin

import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.model.Message as SqsMessage
import aws.sdk.kotlin.services.sqs.model.MessageSystemAttributeName
import aws.sdk.kotlin.services.sqs.receiveMessage
import de.joekoe.sqs.FifoMessageImpl
import de.joekoe.sqs.Message
import de.joekoe.sqs.MessageImpl
import de.joekoe.sqs.Queue
import kotlin.time.Duration

internal suspend fun SqsClient.receiveMessages(
    queue: Queue,
    timeout: Duration,
): List<Message<String>> {
    val response = receiveMessage {
        maxNumberOfMessages = SQS_BATCH_SIZE
        waitTimeSeconds = timeout.inWholeSeconds.toInt()
        queueUrl = queue.url.value
        messageAttributeNames = listOf("*")
    }

    return response.messages.orEmpty().map { message ->
        if (queue is Queue.Fifo) {
            toFifoMessage(message, queue)
        } else {
            toMessage(message, queue)
        }
    }
}

private fun toMessage(message: SqsMessage, queue: Queue) =
    MessageImpl(
        id = Message.Id(message.messageId!!),
        receiptHandle = Message.ReceiptHandle(message.receiptHandle!!),
        attributes = message.stringAttributes(),
        content = message.body.orEmpty(),
        queue = queue,
    )

private fun toFifoMessage(message: SqsMessage, queue: Queue.Fifo): FifoMessageImpl<String> {
    val attrs = message.stringAttributes()
    return FifoMessageImpl(
        id = Message.Id(message.messageId!!),
        receiptHandle = Message.ReceiptHandle(message.receiptHandle!!),
        attributes = attrs,
        content = message.body.orEmpty(),
        groupId = Message.Fifo.GroupId(attrs.getValue(MessageSystemAttributeName.MessageGroupId.value)),
        deduplicationId =
            Message.Fifo.DeduplicationId(attrs.getValue(MessageSystemAttributeName.MessageDeduplicationId.value)),
        queue = queue,
    )
}

private fun SqsMessage.stringAttributes() =
    messageAttributes
        .orEmpty()
        .asSequence()
        .filter { (_, v) -> v.dataType != "Binary" }
        .mapNotNull { (k, v) -> v.stringValue?.let { k to it } }
        .toMap()

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
        maxNumberOfMessages = KotlinSqsConnector.BATCH_SIZE
        waitTimeSeconds = timeout.inWholeSeconds.toInt()
        queueUrl = queue.url.value
        messageAttributeNames = listOf("*")
    }

    return response.messages.orEmpty().map(if (queue is Queue.Fifo) ::toFifoMessage else ::toMessage)
}

private fun toMessage(message: SqsMessage) =
    MessageImpl(
        id = Message.Id(message.messageId!!),
        receiptHandle = Message.ReceiptHandle(message.receiptHandle!!),
        attributes = message.stringAttributes(),
        content = message.body,
    )

private fun toFifoMessage(message: SqsMessage): FifoMessageImpl<String> {
    val attrs = message.stringAttributes()
    return FifoMessageImpl(
        id = Message.Id(message.messageId!!),
        receiptHandle = Message.ReceiptHandle(message.receiptHandle!!),
        attributes = attrs,
        content = message.body,
        groupId = Message.Fifo.GroupId(attrs.getValue(MessageSystemAttributeName.MessageGroupId.value)),
        deduplicationId =
            Message.Fifo.DeduplicationId(attrs.getValue(MessageSystemAttributeName.MessageDeduplicationId.value)),
    )
}

private fun SqsMessage.stringAttributes() =
    messageAttributes
        .orEmpty()
        .asSequence()
        .filter { (_, v) -> v.dataType != "Binary" }
        .mapNotNull { (k, v) -> v.stringValue?.let { k to it } }
        .toMap()

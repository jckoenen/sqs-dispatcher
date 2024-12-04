package de.joekoe.sqs.impl.kotlin

import aws.sdk.kotlin.services.sqs.SqsClient
import com.fasterxml.jackson.databind.ObjectMapper
import de.joekoe.sqs.Message
import de.joekoe.sqs.OutboundMessage
import de.joekoe.sqs.Queue
import de.joekoe.sqs.SqsConnector
import kotlin.time.Duration

internal class KotlinSqsConnector(
    private val sqsClient: SqsClient,
    private val json: ObjectMapper,
    private val options: SqsConnector.Options,
) : SqsConnector {
    override suspend fun getQueue(name: Queue.Name): Queue? = sqsClient.getQueue(json, name, options)

    override suspend fun getOrCreateQueue(name: Queue.Name, createDlq: Boolean): Queue =
        sqsClient.getOrCreateQueue(json, name, createDlq, options)

    override suspend fun receiveMessages(queue: Queue, timeout: Duration): List<Message<String>> =
        sqsClient.receiveMessages(queue, timeout)

    override suspend fun <T : Any> sendMessages(
        queue: Queue,
        messages: Collection<OutboundMessage<T>>,
    ): List<SqsConnector.FailedBatchEntry<OutboundMessage<T>>> = sqsClient.sendMessages(json, queue, messages)

    override suspend fun deleteMessages(
        queue: Queue,
        messages: Collection<Message.ReceiptHandle>,
    ): List<SqsConnector.FailedBatchEntry<Message.ReceiptHandle>> = sqsClient.deleteMessages(queue, messages)

    override suspend fun extendMessageVisibility(
        queue: Queue,
        messages: Collection<Message.ReceiptHandle>,
        duration: Duration,
    ): List<SqsConnector.FailedBatchEntry<Message.ReceiptHandle>> =
        sqsClient.extendMessageVisibility(queue, messages, duration)
}

package de.joekoe.sqs.impl.kotlin

import aws.sdk.kotlin.services.sqs.SqsClient
import com.fasterxml.jackson.databind.ObjectMapper
import de.joekoe.sqs.Message
import de.joekoe.sqs.Queue
import de.joekoe.sqs.SqsConnector
import kotlin.time.Duration
import org.slf4j.Logger
import org.slf4j.LoggerFactory

internal class KotlinSqsConnector(
    private val sqsClient: SqsClient,
    private val json: ObjectMapper,
    private val options: SqsConnector.Options,
) : SqsConnector {
    companion object {
        val logger: Logger = LoggerFactory.getLogger(KotlinSqsConnector::class.java)

        const val BATCH_SIZE = 10
    }

    override suspend fun getQueue(name: Queue.Name): Queue? = sqsClient.getQueue(json, name, options)

    override suspend fun getOrCreateQueue(name: Queue.Name, createDlq: Boolean): Queue =
        sqsClient.getOrCreateQueue(json, name, createDlq, options)

    override suspend fun receiveMessages(queue: Queue, timeout: Duration): List<Message<String>> =
        sqsClient.receiveMessages(queue, timeout)

    override suspend fun <T : Any> sendMessages(
        queue: Queue,
        messages: List<Message<T>>,
    ): List<SqsConnector.FailedBatchEntry<T>> = sqsClient.sendMessages(json, queue, messages)

    override suspend fun <T : Any> deleteMessages(
        queue: Queue,
        messages: List<Message<T>>,
    ): List<SqsConnector.FailedBatchEntry<T>> = sqsClient.deleteMessages(queue, messages)

    override suspend fun <T : Any> extendMessageVisibility(
        queue: Queue,
        messages: List<Message<T>>,
        duration: Duration,
    ): List<SqsConnector.FailedBatchEntry<T>> = sqsClient.extendMessageVisibility(queue, messages, duration)
}

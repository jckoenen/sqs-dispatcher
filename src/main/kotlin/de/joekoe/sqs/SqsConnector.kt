package de.joekoe.sqs

import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import org.slf4j.LoggerFactory

interface SqsConnector {

    data class Options(val defaultVisibilityTimeout: Duration = 30.seconds)

    data class FailedBatchEntry<T : Any>(
        val reference: T,
        val code: String,
        val errorMessage: String?,
        val senderFault: Boolean,
    )

    companion object {
        internal val logger = LoggerFactory.getLogger(SqsConnector::class.java)
    }

    suspend fun getQueue(name: Queue.Name): Queue?

    suspend fun getOrCreateQueue(name: Queue.Name, createDlq: Boolean = false): Queue

    suspend fun receiveMessages(queue: Queue, timeout: Duration = 10.seconds): List<Message<String>>

    suspend fun <T : Any> sendMessages(
        queueUrl: Queue.Url,
        messages: Collection<OutboundMessage<T>>,
    ): List<FailedBatchEntry<OutboundMessage<T>>>

    suspend fun deleteMessages(
        queueUrl: Queue.Url,
        messages: Collection<Message.ReceiptHandle>,
    ): List<FailedBatchEntry<Message.ReceiptHandle>>

    suspend fun extendMessageVisibility(
        queueUrl: Queue.Url,
        messages: Collection<Message.ReceiptHandle>,
        duration: Duration,
    ): List<FailedBatchEntry<Message.ReceiptHandle>>
}

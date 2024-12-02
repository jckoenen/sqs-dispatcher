package de.joekoe.sqs

import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

interface SqsConnector {
    data class Options(val defaultVisibilityTimeout: Duration = 30.seconds)

    data class FailedBatchEntry<T : Any>(
        val message: Message<T>,
        val code: String,
        val errorMessage: String?,
        val senderFault: Boolean,
    )

    suspend fun getQueue(name: Queue.Name): Queue?

    suspend fun getOrCreateQueue(name: Queue.Name, createDlq: Boolean = false): Queue

    suspend fun receiveMessages(queue: Queue, timeout: Duration = 10.seconds): List<Message<String>>

    suspend fun <T : Any> sendMessages(queue: Queue, messages: List<Message<T>>): List<FailedBatchEntry<T>>

    suspend fun <T : Any> deleteMessages(queue: Queue, messages: List<Message<T>>): List<FailedBatchEntry<T>>
}

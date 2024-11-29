package de.joekoe.sqs

import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

interface SqsConnector {
    data class Options(val defaultVisibilityTimeout: Duration = 30.seconds)

    suspend fun getQueue(name: Queue.Name): Queue?

    suspend fun getOrCreateQueue(name: Queue.Name, createDlq: Boolean = false): Queue
}

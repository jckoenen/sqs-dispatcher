package de.joekoe.sqs

import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

interface SqsConnector {
    data class Options(
        val defaultVisibilityTimeout: Duration = 30.seconds,
    )

    sealed interface Failure {
        data object PermissionDenied : Failure

        data object QueueAlreadyExists : Failure
    }

    suspend fun getQueue(name: Queue.Name): Queue?

    suspend fun createQueue(name: Queue.Name, createDlq: Boolean): Queue
}

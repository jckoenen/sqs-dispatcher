package de.joekoe.sqs.flow

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.Flow

/**
 * Represents a mechanism to control and manage draining operations within a coroutine-based context. Implementations of
 * this interface are expected to define the behavior for draining specific resources or flows of data.
 *
 * @property job A reference to the coroutine's [Job] that facilitates lifecycle management and structured concurrency
 *   for the draining operation.
 */
sealed interface DrainControl {
    val job: Job

    fun drain()
}

/**
 * Represents a flow that can be terminated gracefully, ensuring all currently in-flight elements are processed before
 * the flow is completely stopped. This allows for clean shutdown scenarios where no data is lost during the termination
 * process.
 */
sealed interface DrainableFlow<T> : Flow<T> {
    fun launchWithDrainControl(scope: CoroutineScope): DrainControl
}

suspend fun DrainControl.drainAndJoin() {
    drain()
    job.join()
}

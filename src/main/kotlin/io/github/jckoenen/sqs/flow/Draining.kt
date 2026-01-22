package io.github.jckoenen.sqs.flow

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.Flow

/**
 * Represents a mechanism to control and manage draining operations within a coroutine-based context. Implementations of
 * this interface define the behaviour for draining specific resources or flows of data.
 *
 * @property job A reference to the coroutine's [Job] that facilitates lifecycle management and structured concurrency.
 */
public sealed interface DrainControl {
    public val job: Job

    /**
     * Signals that the operation should stop accepting new work and complete once all currently in-flight items are
     * processed.
     */
    public fun drain()
}

/**
 * Represents a flow that can be terminated gracefully, ensuring all currently in-flight elements are processed before
 * the flow is completely stopped. This allows for clean shutdown scenarios where no data is lost during the termination
 * process.
 */
public sealed interface DrainableFlow<T> : Flow<T> {
    /**
     * Launches the flow in the provided [scope] and returns a [DrainControl] to manage its lifecycle.
     *
     * @param scope the scope in which to launch the flow
     * @return a [DrainControl] for managing the flow
     */
    public fun launchWithDrainControl(scope: CoroutineScope): DrainControl
}

/** Signals that the operation should drain and waits for the underlying job to complete. */
public suspend fun DrainControl.drainAndJoin() {
    drain()
    job.join()
}

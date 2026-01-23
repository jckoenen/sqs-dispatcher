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

/**
 * Converts this [Flow] into a [DrainableFlow], enabling graceful termination through [DrainControl].
 *
 * When [DrainControl.drain] is invoked on the control object returned by [DrainableFlow.launchWithDrainControl]:
 * - The upstream of the [drainable] operator is cancelled.
 * - Elements already in the flow's pipeline continue to be processed by downstream operators.
 * - If multiple [drainable] operators are used in a chain, only the one closest to the source (the first one applied to
 *   the source flow) is cancelled, which ensures the entire pipeline can drain its remaining elements.
 *
 * @param T the type of elements in the flow.
 * @return a [DrainableFlow] that supports graceful draining.
 * @see DrainControl.drain
 * @see DrainableFlow.launchWithDrainControl
 */
public fun <T> Flow<T>.drainable(): DrainableFlow<T> = drainableImpl()

/** Signals that the operation should drain and waits for the underlying job to complete. */
public suspend fun DrainControl.drainAndJoin() {
    drain()
    job.join()
}

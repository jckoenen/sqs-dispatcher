package io.github.jckoenen.flow

import io.github.jckoenen.MessageConsumer
import io.github.jckoenen.Queue
import io.github.jckoenen.SqsConnector
import io.github.jckoenen.utils.resolveQueue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.onEach

// These are just best guess
private const val CHUNK_WINDOW_FACTOR = .6
private const val VISIBILITY_OFFSET_FACTOR = 0.2

fun SqsConnector.consume(
    queue: Queue,
    consumer: MessageConsumer,
    enableAutomaticVisibilityExtension: Boolean = true,
    visibilityTimeout: Duration = 30.seconds,
): DrainableFlow<Nothing> = drainable {
    check(visibilityTimeout.isFinite() && visibilityTimeout.isPositive()) {
        "visibilityTimeout must be finite and positive, got $visibilityTimeout"
    }

    val visibilityManager =
        if (enableAutomaticVisibilityExtension) {
            VisibilityManager(this@consume, visibilityTimeout, visibilityTimeout * VISIBILITY_OFFSET_FACTOR)
        } else {
            null
        }

    receive(queue, visibilityTimeout)
        .maybe { visibilityManager?.trackInbound(it) }
        .applyConsumer(consumer, chunkWindow = visibilityTimeout * CHUNK_WINDOW_FACTOR)
        .onEach { applyMessageActions(it, queue) }
        .maybe { visibilityManager?.trackOutbound(it) }
        .collect {}
}

fun SqsConnector.consume(
    queueName: Queue.Name,
    consumer: MessageConsumer,
    enableAutomaticVisibilityExtension: Boolean = true,
    visibilityTimeout: Duration = 30.seconds,
): DrainableFlow<Nothing> = drainable {
    val q = resolveQueue(queueName)

    consume(q, consumer, enableAutomaticVisibilityExtension, visibilityTimeout).collect(::emit)
}

private inline fun <T> Flow<T>.maybe(f: (Flow<T>) -> Flow<T>?): Flow<T> = f(this) ?: this

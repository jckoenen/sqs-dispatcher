package io.github.jckoenen.flow

import io.github.jckoenen.MessageConsumer
import io.github.jckoenen.Queue
import io.github.jckoenen.SqsConnector
import io.github.jckoenen.utils.resolveQueue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.onEach

fun SqsConnector.consume(
    queue: Queue,
    consumer: MessageConsumer,
    visibilityTimeout: Duration = 30.seconds,
    automaticVisibilityExtension: Duration? = visibilityTimeout,
): DrainableFlow<Nothing> = drainable {
    check(visibilityTimeout.isFinite() && visibilityTimeout.isPositive()) {
        "visibilityTimeout must be finite and positive, got $visibilityTimeout"
    }

    val visibilityManager =
        if (automaticVisibilityExtension != null) {
            check(automaticVisibilityExtension.isFinite() && automaticVisibilityExtension.isPositive()) {
                "automaticVisibilityExtension must be finite and positive, got $automaticVisibilityExtension"
            }
            VisibilityManager(this@consume, automaticVisibilityExtension)
        } else {
            null
        }

    receive(queue, visibilityTimeout)
        .maybe { visibilityManager?.trackInbound(it) }
        .applyConsumer(consumer)
        .onEach { applyMessageActions(it, queue) }
        .maybe { visibilityManager?.trackOutbound(it) }
        .collect {}
}

fun SqsConnector.consume(
    queueName: Queue.Name,
    consumer: MessageConsumer,
    visibilityTimeout: Duration = 30.seconds,
    automaticVisibilityExtension: Duration? = visibilityTimeout,
): DrainableFlow<Nothing> = drainable {
    val q = resolveQueue(queueName)

    consume(q, consumer, visibilityTimeout, automaticVisibilityExtension).collect(::emit)
}

private inline fun <T> Flow<T>.maybe(f: (Flow<T>) -> Flow<T>?): Flow<T> = f(this) ?: this

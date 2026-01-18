package de.joekoe.sqs.flow

import kotlin.coroutines.CoroutineContext
import kotlin.experimental.ExperimentalTypeInference
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory

fun <T> Flow<T>.drainable(): DrainableFlow<T> =
    channelFlow {
            val upstream = onEach(::send).launchIn(this)
            val drain = currentCoroutineContext()[DrainControlElement]

            if (drain != null) {
                drain.signal.await()
                upstream.cancelAndJoin()
            } else {
                LoggerFactory.getLogger(DrainableFlow::class.java)
                    .warn("Drainable flow did not have a drain signal attached")
            }
        }
        .let(::DrainableFlowImpl)

@OptIn(ExperimentalTypeInference::class)
@BuilderInference
internal fun <T> drainable(f: suspend FlowCollector<T>.() -> Unit) = flow(f).drainable()

@JvmInline
internal value class DrainableFlowImpl<T>(private val delegate: Flow<T>) : DrainableFlow<T> {
    override suspend fun collect(collector: FlowCollector<T>) = delegate.collect(collector)

    override fun launchWithDrainControl(scope: CoroutineScope): DrainControl {
        val element = DrainControlElement(CompletableDeferred())
        val job = scope.launch(element) { collect() }
        return DrainControlImpl(job, element)
    }
}

private data class DrainControlElement(val signal: CompletableDeferred<Unit>) : CoroutineContext.Element {
    override val key: CoroutineContext.Key<*>
        get() = DrainControlElement

    companion object : CoroutineContext.Key<DrainControlElement>
}

private data class DrainControlImpl(
    override val job: Job,
    private val element: DrainControlElement,
) : DrainControl {
    override fun drain() {
        element.signal.complete(Unit)
    }
}

package de.joekoe.sqs.flow

import kotlin.coroutines.CoroutineContext
import kotlin.experimental.ExperimentalTypeInference
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch

internal fun drainSource(): Flow<Unit> = flow {
    val control = currentCoroutineContext()[DrainControlElement]
    while (control?.active != false) emit(Unit)
}

internal fun <T> Flow<T>.drainable(): DrainableFlow<T> = DrainableFlowImpl(this)

@OptIn(ExperimentalTypeInference::class)
@BuilderInference
internal fun <T> drainable(f: suspend FlowCollector<T>.() -> Unit) = flow(f).drainable()

@JvmInline
internal value class DrainableFlowImpl<T>(private val delegate: Flow<T>) : DrainableFlow<T> {
    override suspend fun collect(collector: FlowCollector<T>) = delegate.collect(collector)

    override fun launchWithDrainControl(scope: CoroutineScope): DrainControl {
        val element = DrainControlElement(true)
        val job = scope.launch(element) { collect() }
        return DrainControlImpl(job, element)
    }
}

private data class DrainControlElement(var active: Boolean) : CoroutineContext.Element {
    override val key: CoroutineContext.Key<*>
        get() = DrainControlElement

    companion object : CoroutineContext.Key<DrainControlElement>
}

private data class DrainControlImpl(
    override val job: Job,
    private val element: DrainControlElement,
) : DrainControl {
    override fun drain() {
        element.active = false
    }
}

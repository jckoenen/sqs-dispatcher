package de.joekoe.sqs.flow

import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch

sealed interface DrainControl {
    val job: Job

    fun drain()
}

suspend fun DrainControl.drainAndJoin() {
    drain()
    job.join()
}

fun drainSource(): Flow<Unit> = flow {
    val control = currentCoroutineContext()[DrainControlElement]
    while (control?.active != false) emit(Unit)
}

fun Flow<Any?>.launchDraining(scope: CoroutineScope): DrainControl {
    val element = DrainControlElement(true)
    val job = scope.launch(element) { collect() }
    return DrainControlImpl(job, element)
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

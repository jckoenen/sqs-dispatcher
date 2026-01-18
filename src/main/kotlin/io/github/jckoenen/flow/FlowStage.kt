package io.github.jckoenen.flow

import io.github.jckoenen.MessageBound
import kotlinx.coroutines.flow.Flow

fun interface FlowStage<in A : MessageBound, out B : MessageBound> {
    fun inbound(upstream: Flow<List<A>>): Flow<List<B>>

    fun <C : MessageBound> outbound(upstream: Flow<List<C>>): Flow<List<C>> = upstream
}

fun <A : MessageBound, B : MessageBound> Flow<List<A>>.through(stage: FlowStage<A, B>): Flow<List<B>> =
    stage.inbound(this).let(stage::outbound)

fun <A : MessageBound, B : MessageBound, C : MessageBound> FlowStage<A, B>.compose(next: FlowStage<B, C>) =
    object : FlowStage<A, C> {
        private val outer = this@compose
        private val inner = next

        override fun inbound(upstream: Flow<List<A>>): Flow<List<C>> = upstream.let(outer::inbound).let(inner::inbound)

        override fun <C : MessageBound> outbound(upstream: Flow<List<C>>): Flow<List<C>> =
            upstream.let(inner::outbound).let(outer::outbound)
    }

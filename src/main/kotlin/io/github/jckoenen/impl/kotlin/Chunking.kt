package io.github.jckoenen.impl.kotlin

import arrow.core.Nel
import arrow.core.toNonEmptyListOrNull
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.fold
import kotlinx.coroutines.flow.mapNotNull

internal const val SQS_BATCH_SIZE = 10

internal inline fun <A : Any, B : Any> Iterable<A>.chunkForBatching(
    crossinline f: (Int, A) -> B
): Flow<Nel<Pair<A, B>>> =
    withIndex()
        .chunked(SQS_BATCH_SIZE) { chunk -> chunk.map { (i, v) -> v to f(i, v) } }
        .asFlow()
        .mapNotNull(List<Pair<A, B>>::toNonEmptyListOrNull)

internal suspend fun <T> Flow<Collection<T>>.flattenToList() = fold(emptyList(), List<T>::plus)

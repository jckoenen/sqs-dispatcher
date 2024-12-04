package de.joekoe.sqs.impl.kotlin

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.fold

internal const val SQS_BATCH_SIZE = 10

internal inline fun <A : Any, B : Any> Iterable<A>.chunkForBatching(
    crossinline f: (Int, A) -> B
): Flow<List<Pair<A, B>>> =
    withIndex().chunked(SQS_BATCH_SIZE) { chunk -> chunk.map { (i, v) -> v to f(i, v) } }.asFlow()

internal suspend fun <T> Flow<Collection<T>>.flattenToList() = fold(emptyList(), List<T>::plus)

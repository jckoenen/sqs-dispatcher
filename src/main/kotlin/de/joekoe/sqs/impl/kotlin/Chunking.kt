package de.joekoe.sqs.impl.kotlin

import de.joekoe.sqs.Message
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.fold
import kotlinx.coroutines.flow.map

internal inline fun <M : Any, C : Any> Collection<Message<M>>.chunkForBatching(crossinline f: (Message<M>) -> C) =
    chunked(KotlinSqsConnector.BATCH_SIZE) { it.associateBy(Message<*>::id) }
        .asFlow()
        .map { chunk -> chunk to chunk.values.map(f) }

internal suspend fun <T> Flow<Collection<T>>.flattenToList() = fold(emptyList(), List<T>::plus)

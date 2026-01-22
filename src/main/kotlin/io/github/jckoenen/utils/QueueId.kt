package io.github.jckoenen.utils

import arrow.core.Ior
import io.github.jckoenen.Queue

internal typealias QueueId = Ior<Queue.Url, Queue.Name>

internal fun Queue.id() = Ior.Both(url, name)

internal fun QueueId.asTags() =
    map { "sqs.queue.name" to it.value }
        .mapLeft { "sqs.queue.url" to it.value }
        .fold(::mapOf, ::mapOf) { l, r -> mapOf(l, r) }

internal fun opTag(operation: String) = mapOf("sqs.operation" to operation)

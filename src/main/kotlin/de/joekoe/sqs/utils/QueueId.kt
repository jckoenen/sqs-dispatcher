package de.joekoe.sqs.utils

import arrow.core.Ior
import de.joekoe.sqs.Queue

internal typealias QueueId = Ior<Queue.Url, Queue.Name>

internal fun Queue.id() = Ior.Both(url, name)

internal fun QueueId.asTags() =
    map { "queue.name" to it.value }.mapLeft { "queue.url" to it.value }.fold(::mapOf, ::mapOf) { l, r -> mapOf(l, r) }

internal fun opTag(operation: String) = mapOf("operation" to operation)

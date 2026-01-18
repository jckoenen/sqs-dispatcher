package io.github.jckoenen

sealed interface Queue {
    @JvmInline value class Name(val value: String)

    @JvmInline value class Url(val value: String)

    val name: Name
    val url: Url
    val dlq: Queue?

    sealed interface Fifo : Queue
}

internal data class QueueImpl(
    override val name: Queue.Name,
    override val url: Queue.Url,
    override val dlq: Queue?,
) : Queue

internal data class FifoQueueImpl(
    override val name: Queue.Name,
    override val url: Queue.Url,
    override val dlq: Queue?,
) : Queue.Fifo

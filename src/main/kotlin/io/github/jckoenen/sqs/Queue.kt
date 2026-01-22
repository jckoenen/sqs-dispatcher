package io.github.jckoenen.sqs

public sealed interface Queue {
    @JvmInline public value class Name(public val value: String)

    @JvmInline public value class Url(public val value: String)

    public val name: Name
    public val url: Url
    public val dlq: Queue?

    public sealed interface Fifo : Queue
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

package io.github.jckoenen.sqs

import io.github.jckoenen.sqs.impl.QueueArn

/** Represents an SQS queue. */
public sealed interface Queue {
    /** The name of the queue. */
    @JvmInline public value class Name(public val value: String)

    /** The URL of the queue. */
    @JvmInline public value class Url(public val value: String)

    /** The name of this queue. */
    public val name: Name
    /** The URL of this queue. */
    public val url: Url
    /** The associated Dead Letter Queue, if any. */
    public val dlq: Queue?

    /** Represents a FIFO (First-In-First-Out) SQS queue. */
    public sealed interface Fifo : Queue
}

internal data class QueueImpl(
    override val name: Queue.Name,
    override val url: Queue.Url,
    override val dlq: Queue?,
    val arn: QueueArn
) : Queue

internal data class FifoQueueImpl(
    override val name: Queue.Name,
    override val url: Queue.Url,
    override val dlq: Queue?,
    val arn: QueueArn
) : Queue.Fifo

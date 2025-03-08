package de.joekoe.sqs

sealed interface Queue {
    @JvmInline value class Name(val value: String)

    @JvmInline value class Url(val value: String)

    val name: Name
    val url: Url
    val dlqUrl: Url?

    sealed interface Fifo : Queue
}

internal data class QueueImpl(
    override val name: Queue.Name,
    override val url: Queue.Url,
    override val dlqUrl: Queue.Url?,
) : Queue

internal data class FifoQueueImpl(
    override val name: Queue.Name,
    override val url: Queue.Url,
    override val dlqUrl: Queue.Url?,
) : Queue.Fifo

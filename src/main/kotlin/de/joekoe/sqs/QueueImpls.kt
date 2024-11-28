package de.joekoe.sqs

import kotlin.time.Duration

internal data class QueueImpl(
    override val name: Queue.Name,
    override val url: Queue.Url,
    override val visibilityTimeout: Duration,
    override val dlqUrl: Queue.Url?,
) : Queue

internal data class FifoQueueImpl(
    override val name: Queue.Name,
    override val url: Queue.Url,
    override val visibilityTimeout: Duration,
    override val dlqUrl: Queue.Url?,
) : Queue, Queue.Fifo

package de.joekoe.sqs

import kotlin.time.Duration

sealed interface Queue {
    @JvmInline value class Name(val value: String)

    @JvmInline value class Url(val value: String)

    val name: Name
    val url: Url
    val visibilityTimeout: Duration
    val dlqUrl: Url?

    sealed interface Fifo
}

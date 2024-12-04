package de.joekoe.sqs

data class OutboundMessage<T : Any>(
    val content: T,
    val attributes: Map<String, String> = emptyMap(),
    val fifo: Fifo<T>? = null,
) {
    data class Fifo<T : Any>(
        override val groupId: Message.Fifo.GroupId,
        override val deduplicationId: Message.Fifo.DeduplicationId,
    ) : Message.Fifo<T>
}

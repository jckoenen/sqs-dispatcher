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

    internal companion object {
        fun <T : Any> fromMessage(message: Message<T>) =
            OutboundMessage(
                content = message.content,
                attributes = message.attributes,
                fifo = (message as? Message.Fifo<*>)?.let { Fifo(it.groupId, it.deduplicationId) },
            )
    }
}

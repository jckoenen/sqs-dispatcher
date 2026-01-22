package io.github.jckoenen.sqs

public data class OutboundMessage(
    val content: String,
    val attributes: Map<String, String> = emptyMap(),
    val fifo: Fifo? = null,
) {
    public data class Fifo(
        override val groupId: Message.Fifo.GroupId,
        override val deduplicationId: Message.Fifo.DeduplicationId,
    ) : Message.Fifo

    internal companion object {
        fun fromMessage(message: Message<String>) =
            OutboundMessage(
                content = message.content,
                attributes = message.attributes,
                fifo = (message as? Message.Fifo)?.let { Fifo(it.groupId, it.deduplicationId) },
            )
    }
}

package io.github.jckoenen.sqs

/**
 * A message to be sent to an SQS queue.
 *
 * @property content the body of the message
 * @property attributes the message attributes
 * @property fifo FIFO-specific settings for the message, if applicable
 */
public data class OutboundMessage(
    val content: String,
    val attributes: Map<String, String> = emptyMap(),
    val fifo: Fifo? = null,
) {
    /**
     * FIFO-specific settings for an outbound message.
     *
     * @property groupId the group identifier for the message
     * @property deduplicationId the deduplication identifier for the message
     */
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

package io.github.jckoenen.sqs

/**
 * A message to be sent to an SQS queue.
 *
 * @property content the body of the message
 * @property attributes the message attributes
 * @property groupId the group identifier for the message
 * @property deduplicationId the deduplication identifier for the message (only applicable to FIFO)
 */
public data class OutboundMessage(
    val content: String,
    val attributes: Map<String, String> = emptyMap(),
    val groupId: Message.GroupId? = null,
    val deduplicationId: Message.Fifo.DeduplicationId? = null,
) {

    internal companion object {
        fun fromMessage(message: Message<String>) =
            OutboundMessage(
                content = message.content,
                attributes = message.attributes,
                groupId = message.groupId,
                deduplicationId = (message as? Message.Fifo)?.deduplicationId,
            )
    }
}

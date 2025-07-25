package de.joekoe.sqs

sealed interface Message<out T : Any> : MessageBound {
    @JvmInline value class Id(val value: String)

    @JvmInline value class ReceiptHandle(val value: String)

    val id: Id
    val attributes: Map<String, String>
    val content: T

    sealed interface Fifo {
        @JvmInline value class GroupId(val value: String)

        @JvmInline value class DeduplicationId(val value: String)

        val groupId: GroupId
        val deduplicationId: DeduplicationId
    }
}

internal data class MessageImpl<T : Any>(
    override val id: Message.Id,
    override val receiptHandle: Message.ReceiptHandle,
    override val attributes: Map<String, String>,
    override val content: T,
    override val queue: Queue,
) : Message<T>

internal data class FifoMessageImpl<T : Any>(
    override val id: Message.Id,
    override val receiptHandle: Message.ReceiptHandle,
    override val attributes: Map<String, String>,
    override val content: T,
    override val queue: Queue.Fifo,
    override val groupId: Message.Fifo.GroupId,
    override val deduplicationId: Message.Fifo.DeduplicationId,
) : Message<T>, Message.Fifo

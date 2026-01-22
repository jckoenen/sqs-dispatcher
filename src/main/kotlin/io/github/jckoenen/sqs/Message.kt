package io.github.jckoenen.sqs

public sealed interface Message<out T : Any> : MessageBound {
    @JvmInline public value class Id(public val value: String)

    @JvmInline public value class ReceiptHandle(public val value: String)

    public val id: Id
    public val attributes: Map<String, String>
    public val content: T

    public sealed interface Fifo {
        @JvmInline public value class GroupId(public val value: String)

        @JvmInline public value class DeduplicationId(public val value: String)

        // TODO: Non Fifo messages can have groupIds now
        public val groupId: GroupId
        public val deduplicationId: DeduplicationId
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

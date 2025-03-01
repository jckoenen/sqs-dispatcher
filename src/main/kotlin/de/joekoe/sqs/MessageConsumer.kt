package de.joekoe.sqs

sealed interface MessageConsumer<T : Any> {
    interface Configuration {
        val parallelism: Int
    }

    sealed interface Action : MessageBound {
        data class DeleteMessage(
            override val queue: Queue,
            override val receiptHandle: Message.ReceiptHandle,
        ) : Action

        data class RetryMessage(
            override val queue: Queue,
            override val receiptHandle: Message.ReceiptHandle,
        ) : Action

        data class MoveMessageToDlq(val message: Message<*>) : Action, MessageBound by message
    }

    val configuration: Configuration

    fun parse(content: String): T // TODO: errors

    interface Individual<T : Any> : MessageConsumer<T> {
        suspend fun handle(message: Message<T>): Action
    }

    interface Batch<T : Any> : MessageConsumer<T> {
        suspend fun handle(messages: List<Message<T>>): List<Action>
    }
}

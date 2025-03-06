package de.joekoe.sqs

sealed interface MessageConsumer {
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

        data class MoveMessageToDlq(val message: Message<String>) : Action, MessageBound by message
    }

    val configuration: Configuration

    interface Individual : MessageConsumer {
        suspend fun handle(message: Message<String>): Action
    }

    interface Batch : MessageConsumer {
        suspend fun handle(messages: List<Message<String>>): List<Action>
    }
}

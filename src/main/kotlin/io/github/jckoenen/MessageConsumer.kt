package io.github.jckoenen

import kotlin.time.Duration

sealed interface MessageConsumer {
    interface Configuration {
        val parallelism: Int
    }

    sealed interface Action : MessageBound {
        data class DeleteMessage(val message: Message<*>) : Action, MessageBound by message

        data class RetryBackoff(val message: Message<*>, val backoffDuration: Duration) :
            Action, MessageBound by message

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

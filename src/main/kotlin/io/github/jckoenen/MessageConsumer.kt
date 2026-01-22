package io.github.jckoenen

import kotlin.time.Duration

public sealed interface MessageConsumer {
    public interface Configuration {
        public val parallelism: Int
    }

    public sealed interface Action : MessageBound {
        public data class DeleteMessage(val message: Message<*>) : Action, MessageBound by message

        public data class RetryBackoff(val message: Message<*>, val backoffDuration: Duration) :
            Action, MessageBound by message

        public data class MoveMessageToDlq(val message: Message<String>) : Action, MessageBound by message
    }

    public val configuration: Configuration

    public interface Individual : MessageConsumer {
        public suspend fun handle(message: Message<String>): Action
    }

    public interface Batch : MessageConsumer {
        public suspend fun handle(messages: List<Message<String>>): List<Action>
    }
}

package de.joekoe.sqs.flow

import de.joekoe.sqs.Message
import de.joekoe.sqs.Queue

sealed interface MessageAction : MessageBound {
    data class DeleteMessage(
        override val queue: Queue,
        override val receiptHandle: Message.ReceiptHandle,
    ) : MessageAction

    data class RetryMessage(
        override val queue: Queue,
        override val receiptHandle: Message.ReceiptHandle,
    ) : MessageAction

    data class MoveMessageToDlq(
        override val queue: Queue,
        override val receiptHandle: Message.ReceiptHandle,
    ) : MessageAction
}

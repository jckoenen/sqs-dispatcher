package de.joekoe.sqs.flow

import de.joekoe.sqs.Message
import de.joekoe.sqs.Queue
import kotlinx.coroutines.flow.Flow

interface MessageBound {
    val queue: Queue
    val receiptHandle: Message.ReceiptHandle
}

interface MessageHandler<U : MessageBound, D : MessageBound> {
    fun handle(flow: Flow<List<U>>): Flow<D>
}

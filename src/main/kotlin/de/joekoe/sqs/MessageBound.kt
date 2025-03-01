package de.joekoe.sqs

interface MessageBound {
    val queue: Queue
    val receiptHandle: Message.ReceiptHandle
}

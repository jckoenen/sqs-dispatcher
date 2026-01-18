package io.github.jckoenen

interface MessageBound {
    val queue: Queue
    val receiptHandle: Message.ReceiptHandle
}

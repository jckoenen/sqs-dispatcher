package io.github.jckoenen.sqs

public interface MessageBound {
    public val queue: Queue
    public val receiptHandle: Message.ReceiptHandle
}

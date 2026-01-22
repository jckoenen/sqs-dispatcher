package io.github.jckoenen

public interface MessageBound {
    public val queue: Queue
    public val receiptHandle: Message.ReceiptHandle
}

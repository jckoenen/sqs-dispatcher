package io.github.jckoenen.sqs

/** Represents an entity that is bound to a specific message in an SQS queue. */
public interface MessageBound {
    /** The queue to which the message belongs. */
    public val queue: Queue
    /** The receipt handle used to perform actions on the message. */
    public val receiptHandle: Message.ReceiptHandle
}

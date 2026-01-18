package io.github.jckoenen

import aws.smithy.kotlin.runtime.SdkBaseException
import io.github.jckoenen.impl.kotlin.CHANGE_OPERATION
import io.github.jckoenen.utils.QueueId
import io.github.jckoenen.utils.asTags
import io.github.jckoenen.utils.opTag

sealed interface SqsFailure : Failure {
    sealed interface GetQueueFailure : SqsFailure

    sealed interface CreateQueueFailure : SqsFailure, GetQueueFailure

    sealed interface ReceiveMessagesFailure : SqsFailure

    sealed interface SendMessagesFailure : SqsFailure

    sealed interface DeleteMessagesFailure : SqsFailure

    sealed interface ChangeMessagesFailure : SqsFailure {
        data class MessageAlreadyDeleted(val queue: QueueId) : ChangeMessagesFailure {
            override val customTags: Map<String, Any>
                get() = opTag(CHANGE_OPERATION) + queue.asTags()

            override val message: String
                get() = "This message was already deleted"
        }
    }

    data class QueueDoesNotExist(
        val operation: String,
        val queue: QueueId,
        override val message: String = "The target queue does not exist"
    ) : ReceiveMessagesFailure, SendMessagesFailure, DeleteMessagesFailure, ChangeMessagesFailure, GetQueueFailure {
        override val customTags: Map<String, Any>
            get() = opTag(operation) + queue.asTags()
    }

    data class UnknownFailure(
        val operation: String,
        val queue: QueueId,
        val cause: Exception,
        val additionalTags: Map<String, Any> = emptyMap(),
    ) :
        GetQueueFailure,
        CreateQueueFailure,
        ReceiveMessagesFailure,
        SendMessagesFailure,
        DeleteMessagesFailure,
        ChangeMessagesFailure {
        override val customTags: Map<String, Any>
            get() = opTag(operation) + additionalTags + queue.asTags()

        override val message: String
            get() = cause.message ?: "Exception did not provide a message"
    }

    data class Throttled(val operation: String, val queue: QueueId) :
        GetQueueFailure,
        CreateQueueFailure,
        ReceiveMessagesFailure,
        SendMessagesFailure,
        DeleteMessagesFailure,
        ChangeMessagesFailure {
        override val customTags: Map<String, Any>
            get() = opTag(operation) + queue.asTags()

        override val message: String
            get() = "Call failed due to AWS throttling"
    }

    data class KmsFailure(val operation: String, val cause: SdkBaseException, val queue: QueueId) :
        ReceiveMessagesFailure, SendMessagesFailure {
        override val customTags: Map<String, Any>
            get() = opTag(operation) + queue.asTags()

        override val message: String
            get() = "Couldn't read messages due to underlying KMS issue"
    }

    data class PartialFailure(val operation: String, val queue: QueueId) :
        SendMessagesFailure, DeleteMessagesFailure, ChangeMessagesFailure {
        override val customTags: Map<String, Any>
            get() = opTag(operation) + queue.asTags()

        override val message: String
            get() = "Some entries in this batch request could not be processed"
    }
}

package de.joekoe.sqs

import arrow.core.Either
import arrow.core.Ior
import arrow.core.Nel
import de.joekoe.sqs.SqsFailure.ChangeMessagesFailure
import de.joekoe.sqs.SqsFailure.CreateQueueFailure
import de.joekoe.sqs.SqsFailure.DeleteMessagesFailure
import de.joekoe.sqs.SqsFailure.GetQueueFailure
import de.joekoe.sqs.SqsFailure.ReceiveMessagesFailure
import de.joekoe.sqs.SqsFailure.SendMessagesFailure
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import org.slf4j.LoggerFactory

/**
 * The outcome of a batch operation, containing failed entries and their causes on the left, successfully sent messages
 * on the right.
 *
 * @param L the underlying failure type. Entries with the same failure will be grouped together
 * @param R the input/output type of the batch operation. Will be the same instances as passed in
 */
typealias BatchResult<L, R> = Ior<FailuresWithCause<L, R>, List<R>>

typealias FailuresWithCause<L, R> = Map<out L, Nel<SqsConnector.FailedBatchEntry<R>>>

interface SqsConnector {

    data class FailedBatchEntry<T : Any>(
        val reference: T,
        val code: String,
        val errorMessage: String?,
        val senderFault: Boolean?,
    )

    companion object {
        internal val logger = LoggerFactory.getLogger(SqsConnector::class.java)
    }

    suspend fun getQueue(name: Queue.Name): Either<GetQueueFailure, Queue>

    suspend fun getOrCreateQueue(
        name: Queue.Name,
        createDlq: Boolean = false,
    ): Either<CreateQueueFailure, Queue>

    suspend fun receiveMessages(
        queue: Queue,
        receiveTimeout: Duration = 10.seconds,
        visibilityTimeout: Duration = 30.seconds
    ): Either<ReceiveMessagesFailure, List<Message<String>>>

    suspend fun sendMessages(
        queueUrl: Queue.Url,
        messages: Collection<OutboundMessage>,
    ): BatchResult<SendMessagesFailure, OutboundMessage>

    suspend fun deleteMessages(
        queueUrl: Queue.Url,
        messages: Collection<Message.ReceiptHandle>,
    ): BatchResult<DeleteMessagesFailure, Message.ReceiptHandle>

    suspend fun extendMessageVisibility(
        queueUrl: Queue.Url,
        messages: Collection<Message.ReceiptHandle>,
        duration: Duration,
    ): BatchResult<ChangeMessagesFailure, Message.ReceiptHandle>
}

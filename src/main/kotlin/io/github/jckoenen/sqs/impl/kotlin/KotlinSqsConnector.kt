package io.github.jckoenen.sqs.impl.kotlin

import arrow.core.Either
import arrow.core.NonEmptyCollection
import aws.sdk.kotlin.services.sqs.SqsClient
import com.fasterxml.jackson.databind.ObjectMapper
import io.github.jckoenen.sqs.BatchResult
import io.github.jckoenen.sqs.Message
import io.github.jckoenen.sqs.OutboundMessage
import io.github.jckoenen.sqs.Queue
import io.github.jckoenen.sqs.SqsConnector
import io.github.jckoenen.sqs.SqsFailure
import io.github.jckoenen.sqs.SqsFailure.ChangeMessagesFailure
import io.github.jckoenen.sqs.SqsFailure.DeleteMessagesFailure
import kotlin.time.Duration

internal class KotlinSqsConnector(
    private val sqsClient: SqsClient,
    private val json: ObjectMapper,
) : SqsConnector {
    override suspend fun getQueue(name: Queue.Name): Either<SqsFailure.GetQueueFailure, Queue> =
        sqsClient.getQueue(json, name)

    override suspend fun getOrCreateQueue(
        name: Queue.Name,
        createDlq: Boolean,
    ): Either<SqsFailure.CreateQueueFailure, Queue> = sqsClient.getOrCreateQueue(json, name, createDlq)

    override suspend fun receiveMessages(
        queue: Queue,
        receiveTimeout: Duration,
        visibilityTimeout: Duration,
    ): Either<SqsFailure.ReceiveMessagesFailure, List<Message<String>>> =
        sqsClient.receiveMessages(queue, receiveTimeout, visibilityTimeout)

    override suspend fun sendMessages(
        queueUrl: Queue.Url,
        messages: NonEmptyCollection<OutboundMessage>,
    ): BatchResult<SqsFailure.SendMessagesFailure, OutboundMessage> = sqsClient.sendMessages(queueUrl, messages)

    override suspend fun deleteMessages(
        queueUrl: Queue.Url,
        messages: NonEmptyCollection<Message.ReceiptHandle>,
    ): BatchResult<DeleteMessagesFailure, Message.ReceiptHandle> = sqsClient.deleteMessages(queueUrl, messages)

    override suspend fun extendMessageVisibility(
        queueUrl: Queue.Url,
        messages: NonEmptyCollection<Message.ReceiptHandle>,
        duration: Duration,
    ): BatchResult<ChangeMessagesFailure, Message.ReceiptHandle> =
        sqsClient.extendMessageVisibility(queueUrl, messages, duration)
}

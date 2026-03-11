package io.github.jckoenen.sqs.impl.kotlin

import arrow.core.Either
import arrow.core.raise.either
import arrow.core.rightIor
import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.getQueueUrl
import io.github.jckoenen.sqs.FifoQueueImpl
import io.github.jckoenen.sqs.Queue
import io.github.jckoenen.sqs.QueueImpl
import io.github.jckoenen.sqs.SqsFailure.GetQueueFailure

private const val GET_OPERATION = "SQS.GetQueue"

internal suspend fun SqsClient.getQueue(
    name: Queue.Name,
): Either<GetQueueFailure, Queue> = either {
    val url =
        execute<GetQueueFailure, _>(convertCommonExceptions(name.rightIor(), GET_OPERATION)) {
                getQueueUrl { queueName = name.value }.queueUrl!!.let(Queue::Url)
            }
            .bind()

    val dlq = getDlq(url).bind()

    if (name.designatesFifo()) {
        check(dlq == null || dlq is Queue.Fifo) { "This is a bug: DLQ of FIFO queue must be FIFO itself" }

        FifoQueueImpl(name, url, dlq)
    } else {
        check(dlq == null || dlq !is Queue.Fifo) { "This is a bug: DLQ of normal queue must be not be FIFO" }

        QueueImpl(name, url, dlq)
    }
}

internal const val FIFO_SUFFIX = ".fifo"

internal fun Queue.Name.designatesFifo() = value.endsWith(FIFO_SUFFIX)

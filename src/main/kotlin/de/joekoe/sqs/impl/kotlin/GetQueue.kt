package de.joekoe.sqs.impl.kotlin

import arrow.core.Either
import arrow.core.raise.either
import arrow.core.rightIor
import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.getQueueUrl
import com.fasterxml.jackson.databind.ObjectMapper
import de.joekoe.sqs.FifoQueueImpl
import de.joekoe.sqs.Queue
import de.joekoe.sqs.QueueImpl
import de.joekoe.sqs.SqsConnector
import de.joekoe.sqs.SqsFailure.GetQueueFailure

private const val GET_OPERATION = "SQS.GetQueue"

internal suspend fun SqsClient.getQueue(
    json: ObjectMapper,
    name: Queue.Name,
    options: SqsConnector.Options,
): Either<GetQueueFailure, Queue> = either {
    val url =
        execute<GetQueueFailure, _>(convertCommonExceptions(name.rightIor(), GET_OPERATION)) {
                getQueueUrl { queueName = name.value }.queueUrl!!.let(Queue::Url)
            }
            .bind()

    val (visibilityTimeout, dlqUrl) = getQueueAttributes(json, url, options).bind()

    if (name.designatesFifo()) {
        FifoQueueImpl(name, url, visibilityTimeout, dlqUrl)
    } else {
        QueueImpl(name, url, visibilityTimeout, dlqUrl)
    }
}

internal const val FIFO_SUFFIX = ".fifo"

internal fun Queue.Name.designatesFifo() = value.endsWith(FIFO_SUFFIX)

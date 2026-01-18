package io.github.jckoenen.impl.kotlin

import arrow.core.Either
import arrow.core.raise.either
import arrow.core.rightIor
import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.getQueueUrl
import com.fasterxml.jackson.databind.ObjectMapper
import io.github.jckoenen.FifoQueueImpl
import io.github.jckoenen.Queue
import io.github.jckoenen.QueueImpl
import io.github.jckoenen.SqsFailure.GetQueueFailure

private const val GET_OPERATION = "SQS.GetQueue"

internal suspend fun SqsClient.getQueue(
    json: ObjectMapper,
    name: Queue.Name,
): Either<GetQueueFailure, Queue> = either {
    val url =
        execute<GetQueueFailure, _>(convertCommonExceptions(name.rightIor(), GET_OPERATION)) {
                getQueueUrl { queueName = name.value }.queueUrl!!.let(Queue::Url)
            }
            .bind()

    val dlqUrl = getDlqUrl(json, url).bind()

    // TODO unify
    val dlq =
        when {
            dlqUrl == null -> null
            name.designatesFifo() -> FifoQueueImpl(Queue.Name("dlq-${name.value}"), dlqUrl, null)
            else -> QueueImpl(Queue.Name("dlq-${name.value}"), dlqUrl, null)
        }

    if (name.designatesFifo()) {
        FifoQueueImpl(name, url, dlq)
    } else {
        QueueImpl(name, url, dlq)
    }
}

internal const val FIFO_SUFFIX = ".fifo"

internal fun Queue.Name.designatesFifo() = value.endsWith(FIFO_SUFFIX)

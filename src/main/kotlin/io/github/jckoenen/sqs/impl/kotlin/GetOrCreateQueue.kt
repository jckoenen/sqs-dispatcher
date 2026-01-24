package io.github.jckoenen.sqs.impl.kotlin

import arrow.core.Either
import arrow.core.leftIor
import arrow.core.raise.either
import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.createQueue
import aws.sdk.kotlin.services.sqs.setQueueAttributes
import io.github.jckoenen.sqs.FifoQueueImpl
import io.github.jckoenen.sqs.Queue
import io.github.jckoenen.sqs.QueueImpl
import io.github.jckoenen.sqs.SqsConnector
import io.github.jckoenen.sqs.SqsFailure
import io.github.jckoenen.sqs.impl.QueueArn
import io.github.jckoenen.sqs.impl.QueueArn.Companion.arn
import io.github.jckoenen.sqs.impl.RedrivePolicy
import io.github.jckoenen.sqs.utils.asTags
import io.github.jckoenen.sqs.utils.id
import io.github.jckoenen.sqs.utils.putAll

private const val CREATE_OP = "SQS.CreateQueue"

internal suspend fun SqsClient.getOrCreateQueue(
    name: Queue.Name,
    createDlq: Boolean,
): Either<SqsFailure.CreateQueueFailure, Queue> = either {
    val targetQueue = doCreateQueue(name).bind()
    val existingDlq = getDlq(targetQueue.url).bind()

    val finalDlqUrl =
        when {
            existingDlq == null && !createDlq -> null
            existingDlq == null -> {
                val created = doCreateQueue(dlqName(name)).bind()
                attachDlq(targetQueue.url, created.arn).bind()
                SqsConnector.logger
                    .atInfo()
                    .putAll(targetQueue.id().asTags())
                    .addKeyValue("sqs.dlq.url", created.id().asTags())
                    .log("Configured DLQ for existing queue")
                created
            }

            createDlq -> existingDlq
            else -> {
                SqsConnector.logger
                    .atWarn()
                    .putAll(targetQueue.id().asTags())
                    .addKeyValue("sqs.dlq.url", existingDlq.id().asTags())
                    .log("Queue already exists with DLQ configured, will not delete DLQ")
                existingDlq
            }
        }

    when (targetQueue) {
        is FifoQueueImpl -> targetQueue.copy(dlq = finalDlqUrl)
        is QueueImpl -> targetQueue.copy(dlq = finalDlqUrl)
    }
}

private suspend fun SqsClient.attachDlq(
    targetQueueUrl: Queue.Url,
    dlqArn: QueueArn,
) =
    execute(unknownFailure("SQS.SetQueueAttributes", targetQueueUrl)) {
        setQueueAttributes {
            queueUrl = targetQueueUrl.value
            attributes =
                buildAttributes(
                    redrivePolicy =
                        RedrivePolicy(
                            maxReceiveCount = 5,
                            deadLetterTargetArn = dlqArn.toString(),
                        ),
                )
        }
    }

private suspend fun SqsClient.doCreateQueue(
    name: Queue.Name,
) = either {
    val url =
        execute(unknownFailure(CREATE_OP, name)) {
                val q = createQueue {
                    queueName = name.value
                    attributes = buildAttributes(isFifo = name.designatesFifo())
                }
                requireNotNull(q.queueUrl) { "Call did not return a url" }
            }
            .map(Queue::Url)
            .bind()

    val arn =
        QueueArn.fromUrl(url)
            .mapLeft { SqsFailure.UnknownFailure(CREATE_OP, url.leftIor(), IllegalArgumentException(it)) }
            .bind()

    val queue =
        if (name.designatesFifo()) {
            FifoQueueImpl(name, url, null, arn)
        } else {
            QueueImpl(name, url, null, arn)
        }
    SqsConnector.logger.atInfo().putAll(queue.id().asTags()).log("Possibly created new queue")

    queue
}

private fun dlqName(source: Queue.Name) =
    if (source.designatesFifo()) {
        Queue.Name(source.value.replace(FIFO_SUFFIX, "_dlq$FIFO_SUFFIX"))
    } else {
        Queue.Name(source.value + "_dlq")
    }

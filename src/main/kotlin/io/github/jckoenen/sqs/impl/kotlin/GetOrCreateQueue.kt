package io.github.jckoenen.sqs.impl.kotlin

import arrow.core.Either
import arrow.core.Ior
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
import io.github.jckoenen.sqs.impl.RedrivePolicy
import io.github.jckoenen.sqs.utils.asTags
import io.github.jckoenen.sqs.utils.putAll

internal suspend fun SqsClient.getOrCreateQueue(
    name: Queue.Name,
    createDlq: Boolean,
): Either<SqsFailure.CreateQueueFailure, Queue> = either {
    val targetQueueUrl = doCreateQueue(name).bind()
    val existingDlqUrl = getDlqUrl(targetQueueUrl).bind()

    val mainQueue = Ior.Both(targetQueueUrl, name)

    val finalDlqUrl =
        when {
            existingDlqUrl == null && !createDlq -> null
            existingDlqUrl == null -> {
                val created = doCreateQueue(dlqName(name)).bind()
                attachDlq(targetQueueUrl, created).bind()
                SqsConnector.logger
                    .atInfo()
                    .putAll(mainQueue.asTags())
                    .addKeyValue("sqs.dlq.url", created.value)
                    .log("Configured DLQ for existing queue")
                created
            }

            createDlq -> existingDlqUrl
            else -> {
                SqsConnector.logger
                    .atWarn()
                    .putAll(mainQueue.asTags())
                    .addKeyValue("sqs.dlq.url", existingDlqUrl.value)
                    .log("Queue already exists with DLQ configured, will not delete DLQ")
                existingDlqUrl
            }
        }

    val dlq =
        when {
            finalDlqUrl == null -> null
            name.designatesFifo() -> FifoQueueImpl(Queue.Name("dlq-${name.value}"), finalDlqUrl, null)
            else -> QueueImpl(Queue.Name("dlq-${name.value}"), finalDlqUrl, null)
        }

    if (name.designatesFifo()) {
        FifoQueueImpl(name, targetQueueUrl, dlq)
    } else {
        QueueImpl(name, targetQueueUrl, dlq)
    }
}

private suspend fun SqsClient.attachDlq(
    targetQueueUrl: Queue.Url,
    created: Queue.Url,
) =
    execute(unknownFailure("SQS.SetQueueAttributes", targetQueueUrl)) {
        setQueueAttributes {
            queueUrl = targetQueueUrl.value
            attributes =
                buildAttributes(
                    redrivePolicy =
                        RedrivePolicy(
                            // TODO: config
                            maxReceiveCount = 5,
                            deadLetterTargetArn = QueueArn.fromUrl(created).toString(),
                        ),
                )
        }
    }

private suspend fun SqsClient.doCreateQueue(
    name: Queue.Name,
) = either {
    val url =
        execute(unknownFailure("SQS.CreateQueue", name)) {
                val q = createQueue {
                    queueName = name.value
                    attributes = buildAttributes(isFifo = name.designatesFifo())
                }
                requireNotNull(q.queueUrl) { "Call did not return a url" }
            }
            .map(Queue::Url)
            .bind()

    val id = Ior.Both(url, name)

    SqsConnector.logger.atInfo().putAll(id.asTags()).log("Possibly created new queue")

    id.leftValue
}

private fun dlqName(source: Queue.Name) =
    if (source.designatesFifo()) {
        Queue.Name(source.value.replace(FIFO_SUFFIX, "_dlq$FIFO_SUFFIX"))
    } else {
        Queue.Name(source.value + "_dlq")
    }

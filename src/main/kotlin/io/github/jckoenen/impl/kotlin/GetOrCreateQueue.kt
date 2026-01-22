package io.github.jckoenen.impl.kotlin

import arrow.core.Either
import arrow.core.Ior
import arrow.core.raise.either
import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.createQueue
import aws.sdk.kotlin.services.sqs.setQueueAttributes
import com.fasterxml.jackson.databind.ObjectMapper
import io.github.jckoenen.FifoQueueImpl
import io.github.jckoenen.Queue
import io.github.jckoenen.QueueImpl
import io.github.jckoenen.SqsConnector
import io.github.jckoenen.SqsFailure
import io.github.jckoenen.impl.QueueArn
import io.github.jckoenen.impl.RedrivePolicy
import io.github.jckoenen.utils.asTags
import io.github.jckoenen.utils.putAll

internal suspend fun SqsClient.getOrCreateQueue(
    json: ObjectMapper,
    name: Queue.Name,
    createDlq: Boolean,
): Either<SqsFailure.CreateQueueFailure, Queue> = either {
    val targetQueueUrl = doCreateQueue(json, name).bind()
    val existingDlqUrl = getDlqUrl(json, targetQueueUrl).bind()

    val mainQueue = Ior.Both(targetQueueUrl, name)

    val finalDlqUrl =
        when {
            existingDlqUrl == null && !createDlq -> null
            existingDlqUrl == null -> {
                val created = doCreateQueue(json, dlqName(name)).bind()
                attachDlq(targetQueueUrl, json, created).bind()
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
    json: ObjectMapper,
    created: Queue.Url,
) =
    execute(unknownFailure("SQS.SetQueueAttributes", targetQueueUrl)) {
        setQueueAttributes {
            queueUrl = targetQueueUrl.value
            attributes =
                buildAttributes(
                    json,
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
    json: ObjectMapper,
    name: Queue.Name,
) = either {
    val url =
        execute(unknownFailure("SQS.CreateQueue", name)) {
                val q = createQueue {
                    queueName = name.value
                    attributes =
                        buildAttributes(
                            json,
                            isFifo = name.designatesFifo(),
                        )
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

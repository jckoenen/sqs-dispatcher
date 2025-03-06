package de.joekoe.sqs.impl.kotlin

import arrow.core.Either
import arrow.core.raise.either
import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.createQueue
import aws.sdk.kotlin.services.sqs.setQueueAttributes
import com.fasterxml.jackson.databind.ObjectMapper
import de.joekoe.sqs.FifoQueueImpl
import de.joekoe.sqs.Queue
import de.joekoe.sqs.QueueImpl
import de.joekoe.sqs.SqsConnector
import de.joekoe.sqs.SqsFailure
import de.joekoe.sqs.impl.QueueArn
import de.joekoe.sqs.impl.RedrivePolicy

internal suspend fun SqsClient.getOrCreateQueue(
    json: ObjectMapper,
    name: Queue.Name,
    createDlq: Boolean,
    options: SqsConnector.Options,
): Either<SqsFailure.CreateQueueFailure, Queue> = either {
    val targetQueueUrl = doCreateQueue(json, name, options).bind()
    val (visibilityTimeout, existingDlqUrl) = getQueueAttributes(json, targetQueueUrl, options).bind()

    val finalDlqUrl =
        when {
            existingDlqUrl == null && !createDlq -> null
            existingDlqUrl == null -> {
                val created = doCreateQueue(json, dlqName(name), options).bind()
                attachDlq(targetQueueUrl, json, created).bind()
                SqsConnector.logger
                    .atInfo()
                    .addKeyValue("queue.name", name.value)
                    .addKeyValue("dlq.url", created.value)
                    .log("Configured DLQ for existing queue")
                created
            }
            createDlq -> existingDlqUrl
            else -> {
                SqsConnector.logger
                    .atWarn()
                    .addKeyValue("queue.name", name.value)
                    .addKeyValue("dlq.url", existingDlqUrl.value)
                    .log("Queue already exists with DLQ configured, will not delete DLQ")
                existingDlqUrl
            }
        }

    if (name.designatesFifo()) {
        FifoQueueImpl(name, targetQueueUrl, visibilityTimeout, finalDlqUrl)
    } else {
        QueueImpl(name, targetQueueUrl, visibilityTimeout, finalDlqUrl)
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
    options: SqsConnector.Options,
) = either {
    val url =
        execute(unknownFailure("SQS.CreateQueue", name)) {
                val q = createQueue {
                    queueName = name.value
                    attributes =
                        buildAttributes(
                            json,
                            isFifo = name.designatesFifo(),
                            visibilityTimeout = options.defaultVisibilityTimeout,
                        )
                }
                requireNotNull(q.queueUrl) { "Call did not return a url" }
            }
            .bind()

    SqsConnector.logger
        .atInfo()
        .addKeyValue("queue.name", name.value)
        .addKeyValue("queue.url", url)
        .log("Possibly created new queue")

    Queue.Url(url)
}

private fun dlqName(source: Queue.Name) =
    if (source.designatesFifo()) {
        Queue.Name(source.value.replace(FIFO_SUFFIX, "_dlq$FIFO_SUFFIX"))
    } else {
        Queue.Name(source.value + "_dlq")
    }

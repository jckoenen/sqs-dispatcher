package de.joekoe.sqs.impl.kotlin

import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.createQueue
import aws.sdk.kotlin.services.sqs.setQueueAttributes
import com.fasterxml.jackson.databind.ObjectMapper
import de.joekoe.sqs.FifoQueueImpl
import de.joekoe.sqs.Queue
import de.joekoe.sqs.QueueImpl
import de.joekoe.sqs.SqsConnector
import de.joekoe.sqs.impl.QueueArn
import de.joekoe.sqs.impl.RedrivePolicy

internal suspend fun SqsClient.getOrCreateQueue(
    json: ObjectMapper,
    name: Queue.Name,
    createDlq: Boolean,
    options: SqsConnector.Options,
): Queue {
    val targetQueueUrl = doCreateQueue(json, name, options)
    val (visibilityTimeout, existingDlqUrl) = getQueueAttributes(json, targetQueueUrl, options)

    val finalDlqUrl =
        when {
            existingDlqUrl == null && !createDlq -> null
            existingDlqUrl == null -> {
                val created = doCreateQueue(json, Queue.Name("dlq_${name.value}"), options)
                setQueueAttributes {
                    queueUrl = targetQueueUrl.value
                    attributes =
                        buildAttributes(
                            json,
                            redrivePolicy =
                                RedrivePolicy(
                                    maxReceiveCount = 5,
                                    deadLetterTargetArn = QueueArn.fromUrl(created).toString(),
                                ),
                        )
                }
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

    return if (name.designatesFifo()) {
        FifoQueueImpl(name, targetQueueUrl, visibilityTimeout, finalDlqUrl)
    } else {
        QueueImpl(name, targetQueueUrl, visibilityTimeout, finalDlqUrl)
    }
}

private suspend fun SqsClient.doCreateQueue(
    json: ObjectMapper,
    name: Queue.Name,
    options: SqsConnector.Options,
): Queue.Url {
    val response = createQueue {
        queueName = name.value
        attributes =
            buildAttributes(
                json,
                isFifo = name.designatesFifo(),
                visibilityTimeout = options.defaultVisibilityTimeout,
            )
    }
    val url = Queue.Url(response.queueUrl!!)

    SqsConnector.logger
        .atInfo()
        .addKeyValue("queue.name", name.value)
        .addKeyValue("queue.url", url.value)
        .log("Possibly created new queue")

    return url
}

package de.joekoe.sqs.impl.kotlin

import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.getQueueUrl
import aws.sdk.kotlin.services.sqs.model.QueueDoesNotExist
import com.fasterxml.jackson.databind.ObjectMapper
import de.joekoe.sqs.FifoQueueImpl
import de.joekoe.sqs.Queue
import de.joekoe.sqs.QueueImpl
import de.joekoe.sqs.SqsConnector

internal suspend fun SqsClient.getQueue(
    json: ObjectMapper,
    name: Queue.Name,
    options: SqsConnector.Options,
): Queue? {
    val url =
        try {
            getQueueUrl { queueName = name.value }.queueUrl?.let(Queue::Url)
        } catch (_: QueueDoesNotExist) {
            null
        }

    if (url == null) {
        SqsConnector.logger.atInfo().addKeyValue("queue.name", name.value).log("Could not resolve queue url")
        return null
    }

    val (visibilityTimeout, dlqUrl) = getQueueAttributes(json, url, options)

    return if (name.designatesFifo()) {
        FifoQueueImpl(name, url, visibilityTimeout, dlqUrl)
    } else {
        QueueImpl(name, url, visibilityTimeout, dlqUrl)
    }
}

internal fun Queue.Name.designatesFifo() = value.endsWith(".fifo")

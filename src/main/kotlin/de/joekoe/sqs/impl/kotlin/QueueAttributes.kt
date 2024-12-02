package de.joekoe.sqs.impl.kotlin

import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.getQueueAttributes
import aws.sdk.kotlin.services.sqs.model.QueueAttributeName
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import de.joekoe.sqs.Queue
import de.joekoe.sqs.SqsConnector
import de.joekoe.sqs.impl.RedrivePolicy
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

internal typealias VisibilityTimeout = Duration

internal suspend fun SqsClient.getQueueAttributes(
    json: ObjectMapper,
    url: Queue.Url,
    options: SqsConnector.Options,
): Pair<VisibilityTimeout, Queue.Url?> {
    val attributes =
        getQueueAttributes {
                queueUrl = url.value
                attributeNames = listOf(QueueAttributeName.VisibilityTimeout, QueueAttributeName.RedrivePolicy)
            }
            .attributes
            .orEmpty()

    val visibilityTimeout =
        attributes[QueueAttributeName.VisibilityTimeout]?.toIntOrNull()?.seconds ?: options.defaultVisibilityTimeout

    val dlqUrl =
        attributes[QueueAttributeName.RedrivePolicy]
            ?.let { json.readValue<RedrivePolicy>(it) }
            ?.targetArn
            ?.let { arn -> "${config.endpointUrl}/${arn.accountId}/${arn.name.value}" }
            ?.let(Queue::Url)

    return visibilityTimeout to dlqUrl
}

internal fun buildAttributes(
    json: ObjectMapper,
    isFifo: Boolean? = null,
    visibilityTimeout: VisibilityTimeout? = null,
    redrivePolicy: RedrivePolicy? = null,
) = buildMap {
    infix fun QueueAttributeName.from(value: String?) {
        if (value != null) put(this, value)
    }

    QueueAttributeName.FifoQueue from isFifo?.takeIf { it }?.toString()
    QueueAttributeName.VisibilityTimeout from visibilityTimeout?.inWholeSeconds?.toString()
    QueueAttributeName.RedrivePolicy from redrivePolicy?.let(json::writeValueAsString)
}

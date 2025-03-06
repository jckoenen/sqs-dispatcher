package de.joekoe.sqs.impl.kotlin

import arrow.core.Either
import arrow.core.raise.either
import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.getQueueAttributes
import aws.sdk.kotlin.services.sqs.model.QueueAttributeName
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import de.joekoe.sqs.Queue
import de.joekoe.sqs.SqsConnector
import de.joekoe.sqs.SqsFailure
import de.joekoe.sqs.impl.RedrivePolicy
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

internal data class QueueAttributes(val visibilityTimeout: Duration, val dlqUrl: Queue.Url?)

internal suspend fun SqsClient.getQueueAttributes(
    json: ObjectMapper,
    url: Queue.Url,
    options: SqsConnector.Options,
): Either<SqsFailure.UnknownFailure, QueueAttributes> = either {
    val attributes =
        execute(unknownFailure("SQS.GetQueueAttributes", url)) {
                getQueueAttributes {
                        queueUrl = url.value
                        attributeNames =
                            listOf(
                                QueueAttributeName.VisibilityTimeout,
                                QueueAttributeName.RedrivePolicy,
                            )
                    }
                    .attributes
                    .orEmpty()
            }
            .bind()

    val visibilityTimeout =
        attributes[QueueAttributeName.VisibilityTimeout]?.toIntOrNull()?.seconds ?: options.defaultVisibilityTimeout

    val dlqUrl =
        attributes[QueueAttributeName.RedrivePolicy]
            ?.let { json.readValue<RedrivePolicy>(it) }
            ?.targetArn
            ?.let { arn -> "${config.endpointUrl}/${arn.accountId}/${arn.name.value}" }
            ?.let(Queue::Url)

    QueueAttributes(visibilityTimeout, dlqUrl)
}

internal fun buildAttributes(
    json: ObjectMapper,
    isFifo: Boolean? = null,
    visibilityTimeout: Duration? = null,
    redrivePolicy: RedrivePolicy? = null,
) = buildMap {
    infix fun QueueAttributeName.from(value: String?) {
        if (value != null) put(this, value)
    }

    QueueAttributeName.FifoQueue from isFifo?.takeIf { it }?.toString()
    QueueAttributeName.VisibilityTimeout from visibilityTimeout?.inWholeSeconds?.toString()
    QueueAttributeName.RedrivePolicy from redrivePolicy?.let(json::writeValueAsString)
}

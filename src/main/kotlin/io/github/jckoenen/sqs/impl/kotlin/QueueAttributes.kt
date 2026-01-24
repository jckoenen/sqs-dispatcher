package io.github.jckoenen.sqs.impl.kotlin

import arrow.core.Either
import arrow.core.raise.either
import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.getQueueAttributes
import aws.sdk.kotlin.services.sqs.model.QueueAttributeName
import io.github.jckoenen.sqs.Queue
import io.github.jckoenen.sqs.SqsFailure
import io.github.jckoenen.sqs.impl.RedrivePolicy
import kotlinx.serialization.json.Json

private val DEFAULT_JSON = Json { ignoreUnknownKeys = true }

internal suspend fun SqsClient.getDlqUrl(
    url: Queue.Url,
): Either<SqsFailure.UnknownFailure, Queue.Url?> = either {
    val attributes =
        execute(unknownFailure("SQS.GetQueueAttributes", url)) {
                getQueueAttributes {
                        queueUrl = url.value
                        attributeNames = listOf(QueueAttributeName.RedrivePolicy)
                    }
                    .attributes
                    .orEmpty()
            }
            .bind()

    attributes[QueueAttributeName.RedrivePolicy]
        ?.let { DEFAULT_JSON.decodeFromString<RedrivePolicy>(it) }
        ?.targetArn
        ?.let { arn -> "${config.endpointUrl}/${arn.accountId}/${arn.name.value}" }
        ?.let(Queue::Url)
}

internal fun buildAttributes(
    isFifo: Boolean? = null,
    redrivePolicy: RedrivePolicy? = null,
) = buildMap {
    infix fun QueueAttributeName.from(value: String?) {
        if (value != null) put(this, value)
    }

    QueueAttributeName.FifoQueue from isFifo?.takeIf { it }?.toString()
    QueueAttributeName.RedrivePolicy from redrivePolicy?.let(DEFAULT_JSON::encodeToString)
}

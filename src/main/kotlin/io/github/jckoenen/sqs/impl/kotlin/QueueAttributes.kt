package io.github.jckoenen.sqs.impl.kotlin

import arrow.core.Either
import arrow.core.raise.either
import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.getQueueAttributes
import aws.sdk.kotlin.services.sqs.model.QueueAttributeName
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import io.github.jckoenen.sqs.Queue
import io.github.jckoenen.sqs.SqsFailure
import io.github.jckoenen.sqs.impl.RedrivePolicy

internal suspend fun SqsClient.getDlqUrl(
    json: ObjectMapper,
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
        ?.let { json.readValue<RedrivePolicy>(it) }
        ?.targetArn
        ?.let { arn -> "${config.endpointUrl}/${arn.accountId}/${arn.name.value}" }
        ?.let(Queue::Url)
}

internal fun buildAttributes(
    json: ObjectMapper,
    isFifo: Boolean? = null,
    redrivePolicy: RedrivePolicy? = null,
) = buildMap {
    infix fun QueueAttributeName.from(value: String?) {
        if (value != null) put(this, value)
    }

    QueueAttributeName.FifoQueue from isFifo?.takeIf { it }?.toString()
    QueueAttributeName.RedrivePolicy from redrivePolicy?.let(json::writeValueAsString)
}

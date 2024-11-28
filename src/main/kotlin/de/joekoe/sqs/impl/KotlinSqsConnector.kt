package de.joekoe.sqs.impl

import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.createQueue
import aws.sdk.kotlin.services.sqs.getQueueAttributes
import aws.sdk.kotlin.services.sqs.getQueueUrl
import aws.sdk.kotlin.services.sqs.model.QueueAttributeName
import aws.sdk.kotlin.services.sqs.model.QueueDoesNotExist
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import de.joekoe.sqs.FifoQueueImpl
import de.joekoe.sqs.Queue
import de.joekoe.sqs.QueueImpl
import de.joekoe.sqs.SqsConnector
import kotlin.time.Duration.Companion.seconds
import org.slf4j.LoggerFactory

internal class KotlinSqsConnector(
    private val sqsClient: SqsClient,
    private val json: ObjectMapper,
    private val options: SqsConnector.Options,
) : SqsConnector {
    private val logger = LoggerFactory.getLogger(javaClass)

    override suspend fun getQueue(name: Queue.Name): Queue? {
        val url =
            try {
                sqsClient.getQueueUrl { queueName = name.value }.queueUrl
            } catch (_: QueueDoesNotExist) {
                null
            }

        if (url == null) {
            logger.atDebug().addKeyValue("queue.name", name.value).log("Could not resolve queue url")
            return null
        }

        val attributes =
            sqsClient
                .getQueueAttributes {
                    queueUrl = url
                    attributeNames =
                        listOf(
                            QueueAttributeName.VisibilityTimeout,
                            QueueAttributeName.RedrivePolicy,
                        )
                }
                .attributes
                .orEmpty()

        val visibilityTimeout =
            attributes[QueueAttributeName.VisibilityTimeout]?.toIntOrNull()?.seconds ?: options.defaultVisibilityTimeout

        val dlqUrl =
            attributes[QueueAttributeName.RedrivePolicy]
                ?.let { json.readValue<RedrivePolicy>(it) }
                ?.targetArn
                ?.let { arn -> "${sqsClient.config.endpointUrl}/${arn.accountId}/${arn.name}" }
                ?.let(Queue::Url)

        return if (name.designatesFifo()) {
            FifoQueueImpl(name, Queue.Url(url), visibilityTimeout, dlqUrl)
        } else {
            QueueImpl(name, Queue.Url(url), visibilityTimeout, dlqUrl)
        }
    }

    override suspend fun createQueue(name: Queue.Name, createDlq: Boolean): Queue {
        suspend fun doCreateQueue(
            name: Queue.Name,
            redrivePolicy: RedrivePolicy? = null,
        ): Queue.Url {
            val response =
                sqsClient.createQueue {
                    queueName = name.value
                    attributes = buildMap {
                        if (name.designatesFifo()) {
                            put(QueueAttributeName.FifoQueue, true.toString())
                        }
                        put(
                            QueueAttributeName.VisibilityTimeout,
                            options.defaultVisibilityTimeout.inWholeSeconds.toString(),
                        )
                        redrivePolicy?.let(json::writeValueAsString)?.let { put(QueueAttributeName.RedrivePolicy, it) }
                    }
                }
            val url = Queue.Url(response.queueUrl!!)

            logger
                .atInfo()
                .addKeyValue("queue.name", name.value)
                .addKeyValue("queue.url", url.value)
                .log("Created new Queue")

            return url
        }

        val dlq =
            if (createDlq) {
                doCreateQueue(Queue.Name("DLQ_${name.value}"), null)
            } else {
                null
            }

        val redrivePolicy = dlq?.let(QueueArn::fromUrl)?.toString()?.let { RedrivePolicy(5, it) }
        val queue = doCreateQueue(name, redrivePolicy)

        return if (name.designatesFifo()) {
            FifoQueueImpl(name, queue, options.defaultVisibilityTimeout, dlq)
        } else {
            QueueImpl(name, queue, options.defaultVisibilityTimeout, dlq)
        }
    }

    private fun Queue.Name.designatesFifo() = value.endsWith(".fifo")
}

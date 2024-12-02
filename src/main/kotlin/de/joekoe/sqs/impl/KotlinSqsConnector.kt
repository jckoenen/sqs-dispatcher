package de.joekoe.sqs.impl

import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.createQueue
import aws.sdk.kotlin.services.sqs.getQueueAttributes
import aws.sdk.kotlin.services.sqs.getQueueUrl
import aws.sdk.kotlin.services.sqs.model.Message as SqsMessage
import aws.sdk.kotlin.services.sqs.model.MessageSystemAttributeName
import aws.sdk.kotlin.services.sqs.model.QueueAttributeName
import aws.sdk.kotlin.services.sqs.model.QueueDoesNotExist
import aws.sdk.kotlin.services.sqs.model.SendMessageBatchRequestEntry
import aws.sdk.kotlin.services.sqs.receiveMessage
import aws.sdk.kotlin.services.sqs.sendMessageBatch
import aws.sdk.kotlin.services.sqs.setQueueAttributes
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import de.joekoe.sqs.FifoMessageImpl
import de.joekoe.sqs.FifoQueueImpl
import de.joekoe.sqs.Message
import de.joekoe.sqs.MessageImpl
import de.joekoe.sqs.Queue
import de.joekoe.sqs.QueueImpl
import de.joekoe.sqs.SqsConnector
import de.joekoe.sqs.map
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.fold
import kotlinx.coroutines.flow.map
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
                sqsClient.getQueueUrl { queueName = name.value }.queueUrl?.let(Queue::Url)
            } catch (_: QueueDoesNotExist) {
                null
            }

        if (url == null) {
            logger.atInfo().addKeyValue("queue.name", name.value).log("Could not resolve queue url")
            return null
        }

        val (visibilityTimeout, dlqUrl) = getQueueAttributes(url)

        return if (name.designatesFifo()) {
            FifoQueueImpl(name, url, visibilityTimeout, dlqUrl)
        } else {
            QueueImpl(name, url, visibilityTimeout, dlqUrl)
        }
    }

    override suspend fun getOrCreateQueue(name: Queue.Name, createDlq: Boolean): Queue {
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
                .log("Possibly created new queue")

            return url
        }

        val targetQueueUrl = doCreateQueue(name, null)
        val (visibilityTimeout, existingDlqUrl) = getQueueAttributes(targetQueueUrl)

        val finalDlqUrl =
            when {
                existingDlqUrl == null && !createDlq -> null
                existingDlqUrl == null -> {
                    val created = doCreateQueue(Queue.Name("dlq_${name.value}"), null)
                    val redrivePolicy =
                        RedrivePolicy(
                            maxReceiveCount = 5,
                            deadLetterTargetArn = QueueArn.fromUrl(created).toString(),
                        )
                    sqsClient.setQueueAttributes {
                        queueUrl = targetQueueUrl.value
                        attributes = mapOf(QueueAttributeName.RedrivePolicy to json.writeValueAsString(redrivePolicy))
                    }
                    logger
                        .atInfo()
                        .addKeyValue("queue.name", name.value)
                        .addKeyValue("dlq.url", created.value)
                        .log("Configured DLQ for existing queue")
                    created
                }
                createDlq -> existingDlqUrl
                else -> {
                    logger
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

    override suspend fun receiveMessages(queue: Queue, timeout: Duration): List<Message<String>> {
        val response =
            sqsClient.receiveMessage {
                maxNumberOfMessages = 10
                waitTimeSeconds = timeout.inWholeSeconds.toInt()
                queueUrl = queue.url.value
                messageAttributeNames = listOf("*")
            }

        return response.messages.orEmpty().map(if (queue is Queue.Fifo) ::toFifoMessage else ::toMessage)
    }

    override suspend fun sendMessages(queue: Queue, messages: List<Message<*>>): List<SqsConnector.SendFailure<*>> =
        messages
            .chunked(10) { chunk -> chunk.map { msg -> msg.map(json::writeValueAsString) } }
            .asFlow()
            .map { chunk ->
                val response =
                    sqsClient.sendMessageBatch {
                        queueUrl = queue.url.value
                        entries =
                            chunk.map { msg ->
                                SendMessageBatchRequestEntry {
                                    id = msg.id.value
                                    messageBody = msg.content
                                    messageDeduplicationId = (msg as? Message.Fifo<*>)?.deduplicationId?.value
                                    messageGroupId = (msg as? Message.Fifo<*>)?.groupId?.value
                                }
                            }
                    }
                if (response.failed.isNotEmpty()) {
                    val byId = chunk.associateBy(Message<*>::id)
                    response.failed.map {
                        SqsConnector.SendFailure(
                            byId.getValue(Message.Id(it.id)),
                            it.code,
                            it.message,
                            it.senderFault,
                        )
                    }
                } else {
                    emptyList()
                }
            }
            .fold(emptyList()) { acc, value -> acc + value }

    private suspend fun getQueueAttributes(url: Queue.Url): Pair<Duration, Queue.Url?> {
        val attributes =
            sqsClient
                .getQueueAttributes {
                    queueUrl = url.value
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
                ?.let { arn -> "${sqsClient.config.endpointUrl}/${arn.accountId}/${arn.name.value}" }
                ?.let(Queue::Url)

        return visibilityTimeout to dlqUrl
    }

    private fun Queue.Name.designatesFifo() = value.endsWith(".fifo")

    private fun toMessage(message: SqsMessage) =
        MessageImpl(
            id = Message.Id(message.messageId!!),
            receiptHandle = Message.ReceiptHandle(message.receiptHandle!!),
            attributes = message.stringAttributes(),
            content = message.body,
        )

    private fun toFifoMessage(message: SqsMessage): FifoMessageImpl<String> {
        val attrs = message.stringAttributes()
        return FifoMessageImpl(
            id = Message.Id(message.messageId!!),
            receiptHandle = Message.ReceiptHandle(message.receiptHandle!!),
            attributes = attrs,
            content = message.body,
            groupId = Message.Fifo.GroupId(attrs.getValue(MessageSystemAttributeName.MessageGroupId.value)),
            deduplicationId =
                Message.Fifo.DeduplicationId(attrs.getValue(MessageSystemAttributeName.MessageDeduplicationId.value)),
        )
    }

    private fun SqsMessage.stringAttributes() =
        messageAttributes
            .orEmpty()
            .asSequence()
            .filter { (_, v) -> v.dataType != "Binary" }
            .mapNotNull { (k, v) -> v.stringValue?.let { k to it } }
            .toMap()
}

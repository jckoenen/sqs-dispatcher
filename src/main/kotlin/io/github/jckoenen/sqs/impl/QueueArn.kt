package io.github.jckoenen.sqs.impl

import arrow.core.Either
import arrow.core.raise.either
import arrow.core.raise.ensure
import aws.sdk.kotlin.services.sqs.SqsClient
import aws.smithy.kotlin.runtime.net.url.Url
import io.github.jckoenen.sqs.FifoQueueImpl
import io.github.jckoenen.sqs.Queue
import io.github.jckoenen.sqs.QueueImpl

@ConsistentCopyVisibility
internal data class QueueArn private constructor(val accountId: String, val region: String, val name: Queue.Name) {

    override fun toString(): String = buildString {
        append("arn")
            .append(':')
            .append("aws")
            .append(':')
            .append("sqs")
            .append(':')
            .append(region)
            .append(':')
            .append(accountId)
            .append(':')
            .append(name.value)
    }

    fun toUrl(config: SqsClient.Config) = Queue.Url("${config.endpointUrl}/${accountId}/${name.value}")

    companion object {
        val Queue.arn
            get() =
                when (this) {
                    is FifoQueueImpl -> arn
                    is QueueImpl -> arn
                }

        fun fromUrl(url: Queue.Url): Either<String, QueueArn> = either {
            val parsed =
                try {
                    Url.parse(url.value)
                } catch (iae: IllegalArgumentException) {
                    raise(iae.message ?: "Cannot parse ${url.value} as URL")
                }
            ensure(parsed.hostAndPort.startsWith("sqs.")) { "URL ${url.value} does not belong to SQS" }
            ensure(parsed.path.segments.size >= 2) { "URL ${url.value} does not contain region" }

            val region =
                with(parsed.hostAndPort) {
                    val first = indexOf('.') + 1
                    val second = indexOf('.', first)
                    substring(first, second)
                }
            val (account, name) = parsed.path.segments.map { it.encoded }

            QueueArn(account, region, Queue.Name(name))
        }

        fun fromString(arn: String): Either<String, QueueArn> = either {
            val components = arn.split(':')
            when {
                components.size < 6 ->
                    raise("Invalid SQS ARN ${arn}. Expected at least 6 parts, got ${components.size}")

                components[2] != "sqs" ->
                    raise("Invalid SQS arn ${arn}. Third component must be 'sqs', but got '${components[2]}'")

                else -> QueueArn(components[4], components[3], Queue.Name(components[5]))
            }
        }
    }
}

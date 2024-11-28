package de.joekoe.sqs.impl

import arrow.core.flatMap
import aws.smithy.kotlin.runtime.net.url.Url
import de.joekoe.sqs.Queue
import java.util.*
import org.slf4j.LoggerFactory

@ConsistentCopyVisibility
internal data class QueueArn private constructor(val accountId: String, val region: String, val name: Queue.Name) {

    override fun toString(): String =
        StringJoiner(":").add("arn").add("aws").add("sqs").add(region).add(accountId).add(name.value).toString()

    fun toUrl() = Queue.Url("https://sqs.$region.amazonaws.com/$accountId/${name.value}")

    companion object {
        private val logger = LoggerFactory.getLogger(QueueArn::class.java)

        fun fromUrl(url: Queue.Url): QueueArn? {
            val parsed =
                runCatching { Url.parse(url.value) }
                    .flatMap {
                        if (!it.hostAndPort.startsWith("sqs.") || it.path.segments.size < 2) {
                            Result.failure(IllegalArgumentException(""))
                        } else {
                            Result.success(it)
                        }
                    }
                    .onFailure { logger.atWarn().addKeyValue("queue.url", url.value).log("Invalid url received") }
                    .getOrNull() ?: return null

            val region =
                with(parsed.hostAndPort) {
                    val first = indexOf('.') + 1
                    val second = indexOf('.', first)
                    substring(first, second)
                }
            val (account, name) = parsed.path.segments.map { it.encoded }

            return QueueArn(account, region, Queue.Name(name))
        }

        fun fromString(arn: String): QueueArn? {
            val components = arn.split(':')
            if (components.size < 6 || components[2] != "sqs") return null
            return QueueArn(components[4], components[3], Queue.Name(components[5]))
        }
    }
}

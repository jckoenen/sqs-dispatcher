package de.joekoe.sqs.testinfra

import aws.sdk.kotlin.runtime.auth.credentials.StaticCredentialsProvider
import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.listQueues
import aws.smithy.kotlin.runtime.net.url.Url
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import de.joekoe.sqs.Queue
import de.joekoe.sqs.SqsConnector
import de.joekoe.sqs.impl.KotlinSqsConnector
import io.kotest.assertions.nondeterministic.eventually
import io.kotest.core.listeners.AfterProjectListener
import io.kotest.core.listeners.BeforeProjectListener
import io.kotest.core.test.TestScope
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import org.testcontainers.containers.localstack.LocalStackContainer
import org.testcontainers.utility.DockerImageName

internal object SqsContainerExtension : BeforeProjectListener, AfterProjectListener {
    private val WORDS_CHARS_ONLY = """\W""".toRegex()
    private val container =
        LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8.1"))
            .withServices(LocalStackContainer.Service.SQS)

    private val scope =
        CoroutineScope(
            CoroutineExceptionHandler { _, ex ->
                LoggerFactory.getLogger(SqsContainerExtension::class.java)
                    .atError()
                    .setCause(ex)
                    .log("Scope closed due to uncaught exception")
            }
        )

    private val client =
        scope.async(start = CoroutineStart.LAZY) {
            withContext(Dispatchers.IO) { container.start() }

            val client =
                SqsClient.fromEnvironment {
                    region = container.region
                    endpointUrl = Url.parse(container.endpoint.toString())
                    credentialsProvider = StaticCredentialsProvider {
                        accessKeyId = container.accessKey
                        secretAccessKey = container.secretKey
                    }
                }
            // localstack is NOT immediately ready, wait for one call to succeed
            eventually(10.seconds) { client.listQueues { maxResults = 1 } }

            client
        }

    @OptIn(ExperimentalCoroutinesApi::class)
    override suspend fun afterProject() {
        super.afterProject()
        if (!client.isCompleted) return
        withContext(Dispatchers.IO) {
            client.getCompleted().close()
            container.stop()
        }
    }

    suspend fun newConnector(clientOverride: SqsClient? = null): SqsConnector =
        KotlinSqsConnector(clientOverride ?: client.await(), jacksonObjectMapper(), SqsConnector.Options())

    fun TestScope.queueName(): Queue.Name =
        testCase.descriptor
            .ids()
            .joinToString(separator = "_") { id -> id.value.substringAfterLast(".") }
            .replace(WORDS_CHARS_ONLY, "_")
            .takeLast(64)
            .let(Queue::Name)

    fun TestScope.fifoQueueName() = Queue.Name(queueName().value + ".fifo")
}

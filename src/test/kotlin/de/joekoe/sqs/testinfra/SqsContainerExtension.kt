package de.joekoe.sqs.testinfra

import aws.sdk.kotlin.runtime.auth.credentials.StaticCredentialsProvider
import aws.sdk.kotlin.services.sqs.SqsClient
import aws.smithy.kotlin.runtime.net.url.Url
import io.kotest.core.listeners.AfterProjectListener
import io.kotest.core.listeners.BeforeProjectListener
import io.kotest.core.test.TestScope
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

            SqsClient.fromEnvironment {
                region = container.region
                endpointUrl = Url.parse(container.endpoint.toString())
                credentialsProvider =
                    StaticCredentialsProvider.invoke {
                        accessKeyId = container.accessKey
                        secretAccessKey = container.secretKey
                    }
            }
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

    fun TestScope.queueName(): String =
        testCase.descriptor
            .ids()
            .joinToString(separator = "-") { id -> id.value.substringAfterLast(".").replace(" ", "_") }
            .takeLast(80)
}

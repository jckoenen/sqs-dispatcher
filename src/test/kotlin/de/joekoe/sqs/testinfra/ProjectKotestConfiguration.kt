package de.joekoe.sqs.testinfra

import io.kotest.core.config.AbstractProjectConfig
import io.kotest.core.extensions.Extension
import io.kotest.extensions.junitxml.JunitXmlReporter
import kotlin.math.min

class ProjectKotestConfiguration : AbstractProjectConfig() {
    override fun extensions(): List<Extension> = listOf(SqsContainerExtension, JunitXmlReporter())

    override val failOnEmptyTestSuite: Boolean = true
    override val parallelism: Int = min(1, Runtime.getRuntime().availableProcessors() - 2)
    override val coroutineDebugProbes: Boolean = true
}

package de.joekoe.sqs.testinfra

import io.kotest.core.config.AbstractProjectConfig
import io.kotest.core.extensions.Extension
import kotlin.math.min

class ProjectKotestConfiguration : AbstractProjectConfig() {
    override fun extensions(): List<Extension> = listOf(SqsContainerExtension)

    override val failOnEmptyTestSuite: Boolean = true
    override val parallelism: Int = min(1, Runtime.getRuntime().availableProcessors() - 2)
}

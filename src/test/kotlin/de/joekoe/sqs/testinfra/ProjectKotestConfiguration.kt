package de.joekoe.sqs.testinfra

import io.kotest.core.config.AbstractProjectConfig
import io.kotest.core.extensions.Extension
import kotlin.math.min
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.debug.DebugProbes

@OptIn(ExperimentalCoroutinesApi::class)
class ProjectKotestConfiguration : AbstractProjectConfig() {
    init {
        DebugProbes.install()
        DebugProbes.sanitizeStackTraces = true
    }

    override fun extensions(): List<Extension> = listOf(SqsContainerExtension)

    override val failOnEmptyTestSuite: Boolean = true
    override val parallelism: Int = min(1, Runtime.getRuntime().availableProcessors() - 2)
}

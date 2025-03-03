package de.joekoe.sqs.testinfra

import io.kotest.assertions.nondeterministic.EventuallyConfiguration
import io.kotest.assertions.nondeterministic.eventuallyConfig
import io.kotest.core.config.AbstractProjectConfig
import io.kotest.core.extensions.Extension
import io.kotest.extensions.junitxml.JunitXmlReporter
import kotlin.math.min
import kotlin.time.Duration.Companion.seconds

class ProjectKotestConfiguration : AbstractProjectConfig() {
    override fun extensions(): List<Extension> = listOf(SqsContainerExtension, JunitXmlReporter())

    override val failOnEmptyTestSuite: Boolean = true
    override val parallelism: Int = min(1, Runtime.getRuntime().availableProcessors() - 2)
    override val coroutineDebugProbes: Boolean = true

    companion object {
        // remove after upgrading kotest to v6.0
        // https://github.com/kotest/kotest/issues/3988
        suspend fun <T> eventually(
            config: EventuallyConfiguration = eventuallyConfig { duration = 10.seconds },
            f: suspend () -> T,
        ) = io.kotest.assertions.nondeterministic.eventually(config, f)
    }
}

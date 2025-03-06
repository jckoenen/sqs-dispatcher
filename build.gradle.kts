import com.diffplug.gradle.spotless.BaseKotlinExtension
import com.github.benmanes.gradle.versions.updates.gradle.GradleReleaseChannel
import org.gradle.api.tasks.testing.logging.TestLogEvent

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.versions)
    alias(libs.plugins.spotless)
    application
}

group = "de.joekoe"

version = "1.0-SNAPSHOT"

repositories { mavenCentral() }

kotlin {
    jvmToolchain(21)
    compilerOptions { optIn.add("kotlinx.coroutines.ExperimentalCoroutinesApi") }
}

dependencies {
    api(libs.kotlin.stdlib)

    api(platform(libs.aws.kotlin.bom))
    api(libs.aws.kotlin.sqs)

    implementation(platform(libs.kotlinx.coroutines.bom))
    api(libs.kotlinx.coroutines.core)
    implementation(libs.kotlinx.coroutines.slf4j)

    implementation(libs.kotlinx.datetime)

    api(platform(libs.jackson.bom))
    api(libs.jackson.databind)
    implementation(libs.jackson.kotlin)

    implementation(platform(libs.arrow.bom))
    api(libs.arrow.core)

    compileOnly(libs.slf4j.api)

    testImplementation(libs.kotlin.reflect)
    testImplementation(libs.logback.classic)
    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(libs.kotlinx.coroutines.debug)

    testImplementation(platform(libs.kotest.bom))
    testImplementation(libs.kotest.runner.junit5)
    testImplementation(libs.kotest.core)
    testImplementation(libs.kotest.property)
    testImplementation(libs.kotest.extensions.testcontainers)
    testImplementation(libs.kotest.extensions.arrow)
    testImplementation(libs.kotest.extensions.xml)

    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.testcontainers.java)
    testImplementation(libs.testcontainers.localstack)
}

tasks {
    test {
        useJUnitPlatform()
        testLogging {
            events = setOf(TestLogEvent.FAILED, TestLogEvent.PASSED, TestLogEvent.SKIPPED)
        }
        reports.junitXml.required = false
        systemProperty(
            "gradle.build.dir",
            project.layout.buildDirectory.map { it.asFile.absolutePath }.get(),
        )
    }

    dependencyUpdates {
        val stableVersion = "^[0-9,.v-]+(-r)?$".toRegex()
        val stableKeywords = listOf("release", "final", "ga")

        fun isStable(version: String): Boolean =
            stableKeywords.any { version.contains(it, ignoreCase = true) } ||
                stableVersion.matches(version)

        gradleReleaseChannel = GradleReleaseChannel.CURRENT.id
        rejectVersionIf { isStable(candidate.version) != isStable(currentVersion) }
    }
}

spotless {
    fun BaseKotlinExtension.defaultFormat() {
        ktfmt().kotlinlangStyle().configure {
            it.setMaxWidth(120)
            it.setBlockIndent(4)
            it.setContinuationIndent(4)
            it.setRemoveUnusedImports(true)
            it.setManageTrailingCommas(false)
        }
        targetExcludeIfContentContains("import io.kotest.core.spec.style.")
    }

    kotlin { defaultFormat() }
    kotlinGradle { defaultFormat() }
}

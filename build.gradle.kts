import com.adarshr.gradle.testlogger.theme.ThemeType
import com.diffplug.gradle.spotless.BaseKotlinExtension
import com.github.benmanes.gradle.versions.updates.gradle.GradleReleaseChannel
import kotlin.time.Duration.Companion.seconds

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.versions)
    alias(libs.plugins.spotless)
    alias(libs.plugins.testlogging)
    application
}

group = "de.joekoe"

version = "1.0-SNAPSHOT"

repositories { mavenCentral() }

kotlin { jvmToolchain(21) }

testlogger {
    theme = ThemeType.MOCHA
    slowThreshold = 1.seconds.inWholeMilliseconds
}

dependencies {
    api(platform(libs.aws.kotlin.bom))
    api(libs.aws.kotlin.sqs)

    implementation(platform(libs.kotlinx.coroutines.bom))
    implementation(libs.kotlinx.coroutines.core)
    implementation(libs.kotlinx.coroutines.slf4j)

    api(platform(libs.jackson.bom))
    api(libs.jackson.databind)
    implementation(libs.jackson.kotlin)

    implementation(platform(libs.arrow.bom))
    implementation(libs.arrow.core)

    compileOnly(libs.slf4j.api)

    testImplementation(libs.logback.classic)

    testImplementation(platform(libs.kotest.bom))
    testImplementation(libs.kotest.runner.junit5)
    testImplementation(libs.kotest.core)
    testImplementation(libs.kotest.extensions.testcontainers)
    testImplementation(libs.kotest.extensions.arrow)

    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.testcontainers.java)
    testImplementation(libs.testcontainers.localstack)
}

tasks {
    test { useJUnitPlatform() }
    dependencyUpdates {
        val stableVersion = "^[0-9,.v-]+(-r)?$".toRegex()
        val stableKeywords = listOf("release", "final", "ga")

        fun isStable(version: String): Boolean =
            stableKeywords.any { version.contains(it, ignoreCase = true) } || stableVersion.matches(version)

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
            it.setRemoveUnusedImport(true)
        }
        targetExcludeIfContentContains("import io.kotest.core.spec.style.")
    }

    kotlin { defaultFormat() }
    kotlinGradle { defaultFormat() }
}

package io.github.jckoenen.sqs.utils

import arrow.core.Either
import arrow.core.getOrElse
import arrow.resilience.Schedule
import arrow.resilience.retryEither
import io.github.jckoenen.sqs.Failure
import io.github.jckoenen.sqs.Queue
import io.github.jckoenen.sqs.SqsConnector
import io.github.jckoenen.sqs.allTags
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

internal suspend fun SqsConnector.resolveQueue(queueName: Queue.Name) =
    retryIndefinitely(10.seconds, 5.minutes) {
        getQueue(queueName).onLeft {
            SqsConnector.logger.atWarn().putAll(it.allTags()).log("Failed to resolve queue, will retry")
        }
    }

internal suspend inline fun <T> retryIndefinitely(
    base: Duration,
    max: Duration,
    f: () -> Either<Failure, T>,
): T =
    Schedule.exponential<Any>(base)
        .doUntil { _, duration -> duration < max }
        .andThen(Schedule.spaced<Any>(max) and Schedule.forever())
        .jittered(min = 0.5, max = 1.5)
        .retryEither(f)
        .getOrElse { error("Indefinite retry exhausted!") }

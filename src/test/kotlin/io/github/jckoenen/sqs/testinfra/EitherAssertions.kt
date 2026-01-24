package io.github.jckoenen.sqs.testinfra

import arrow.core.Either
import arrow.core.Ior
import io.kotest.assertions.arrow.core.shouldBeRight
import io.kotest.matchers.Matcher
import io.kotest.matchers.MatcherResult

fun <T> Either<*, T>.assumeRight(): T = shouldBeRight { "Setup Failure! Expected Either.Right, but got $this" }

fun <T> Ior<*, T>.assumeRight(): T = shouldBeRight { "Setup Failure! Expected Ior.Right, but got $this" }

inline fun <T> beRight(crossinline testFn: T.() -> Unit) =
    object : Matcher<Either<*, T>> {
        override fun test(value: Either<*, T>): MatcherResult {
            value.shouldBeRight().testFn()

            return MatcherResult(true, { "" }, { "" })
        }
    }

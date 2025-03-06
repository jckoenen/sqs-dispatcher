package de.joekoe.sqs.impl.kotlin

import arrow.core.Either
import arrow.core.Ior
import arrow.core.Nel
import arrow.core.combine
import arrow.core.left
import arrow.core.raise.Raise
import arrow.core.recover
import arrow.core.right
import arrow.core.rightIor
import arrow.core.separateEither
import arrow.core.toNonEmptyListOrNull
import aws.sdk.kotlin.services.sqs.model.BatchResultErrorEntry
import aws.sdk.kotlin.services.sqs.model.KmsAccessDenied
import aws.sdk.kotlin.services.sqs.model.KmsDisabled
import aws.sdk.kotlin.services.sqs.model.KmsInvalidKeyUsage
import aws.sdk.kotlin.services.sqs.model.KmsInvalidState
import aws.sdk.kotlin.services.sqs.model.KmsNotFound
import aws.sdk.kotlin.services.sqs.model.KmsOptInRequired
import aws.sdk.kotlin.services.sqs.model.KmsThrottled
import aws.sdk.kotlin.services.sqs.model.OverLimit
import aws.sdk.kotlin.services.sqs.model.QueueDoesNotExist
import aws.sdk.kotlin.services.sqs.model.RequestThrottled
import aws.smithy.kotlin.runtime.SdkBaseException
import de.joekoe.sqs.BatchResult
import de.joekoe.sqs.Queue
import de.joekoe.sqs.SqsConnector
import de.joekoe.sqs.SqsFailure
import de.joekoe.sqs.utils.QueueId
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.fold

internal inline fun <E, T> execute(catch: Raise<E>.(SdkBaseException) -> T, call: () -> T) =
    Either.catchOrThrow<SdkBaseException, _>(call).recover(catch)

internal inline fun <reified E : SqsFailure> convertCommonExceptions(
    queue: QueueId,
    operation: String,
): Raise<E>.(cause: SdkBaseException) -> Nothing = { cause ->
    val failure =
        when (cause) {
            is QueueDoesNotExist -> SqsFailure.QueueDoesNotExist(operation, queue)
            is OverLimit,
            is RequestThrottled -> SqsFailure.Throttled(operation, queue)

            is KmsAccessDenied,
            is KmsNotFound,
            is KmsInvalidState,
            is KmsInvalidKeyUsage,
            is KmsThrottled,
            is KmsDisabled,
            is KmsOptInRequired -> SqsFailure.KmsFailure(operation, cause, queue)

            else -> SqsFailure.UnknownFailure(operation, queue, cause)
        }
    if (failure is E) {
        raise(failure)
    } else {
        throw IllegalStateException(
            "UnknownFailure does not extend ${E::class} - this is a bug!",
            cause,
        )
    }
}

internal fun unknownFailure(operation: String, name: Queue.Name) = failureImpl(operation, name)

internal fun unknownFailure(operation: String, url: Queue.Url) = failureImpl(operation, url = url)

private fun failureImpl(
    operation: String,
    name: Queue.Name? = null,
    url: Queue.Url? = null,
): Raise<SqsFailure.UnknownFailure>.(Exception) -> Nothing = {
    raise(SqsFailure.UnknownFailure(operation, QueueId.fromNullables(url, name)!!, it))
}

internal fun <L, R : Any> emptyResult(): BatchResult<L, R> = emptyList<R>().rightIor()

internal suspend fun <L, R : Any> Flow<BatchResult<L, R>>.combine(): BatchResult<L, R> =
    fold(emptyResult()) { acc, value -> acc.combine(value, { a, v -> a + v }, { a, v -> a + v }) }

internal fun <T : Any> splitFailureAndSuccess(
    operation: String,
    queue: QueueId,
    source: Nel<T>,
    failed: List<BatchResultErrorEntry>,
): BatchResult<SqsFailure.PartialFailure, T> {
    val f = failed.associateBy { it.id }

    return source
        .mapIndexed { index: Int, t: T ->
            val failure = f[index.toString()]
            if (failure == null) {
                t.right()
            } else {
                SqsConnector.FailedBatchEntry(
                        reference = t,
                        code = failure.code,
                        errorMessage = failure.message,
                        senderFault = failure.senderFault,
                    )
                    .left()
            }
        }
        .separateEither()
        .let { (l, r) -> Ior.fromNullables(l.toNonEmptyListOrNull(), r.toNonEmptyListOrNull()) }
        ?.mapLeft { mapOf(SqsFailure.PartialFailure(operation, queue) to it) }
        ?: error("Source is Nel, so one side has to be present")
}

internal fun <E, T : Any> batchCallFailed(failure: E, batch: Nel<T>) =
    mapOf(
        failure to
            batch.map {
                SqsConnector.FailedBatchEntry(
                    it,
                    "CALL_FAILED",
                    "The batch call failed completely",
                    null,
                )
            })

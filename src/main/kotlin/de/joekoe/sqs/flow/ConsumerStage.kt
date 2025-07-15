package de.joekoe.sqs.flow

import arrow.core.Either
import arrow.core.getOrElse
import de.joekoe.sqs.Message
import de.joekoe.sqs.MessageConsumer
import de.joekoe.sqs.MessageConsumer.Action.RetryBackoff
import de.joekoe.sqs.SqsConnector
import de.joekoe.sqs.impl.kotlin.SQS_BATCH_SIZE
import de.joekoe.sqs.utils.chunked
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flatMapMerge
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map

private typealias ConsumerStage = FlowStage<Message<String>, MessageConsumer.Action>

private val CHUNK_TIMEOUT = 30.seconds
private val EXCEPTION_BACKOFF = 1.minutes

internal fun MessageConsumer.asStage(): ConsumerStage =
    when (this) {
        is MessageConsumer.Individual ->
            FlowStage { upstream ->
                upstream
                    .flatMapMerge(configuration.parallelism, List<Message<String>>::asFlow)
                    .map(::handleSafely)
                    .chunked(SQS_BATCH_SIZE, CHUNK_TIMEOUT)
            }
        is MessageConsumer.Batch ->
            FlowStage { upstream ->
                upstream.flatMapMerge(configuration.parallelism) { batch -> flow { emit(handleSafely(batch)) } }
            }
    }

private suspend fun MessageConsumer.Individual.handleSafely(message: Message<String>) =
    Either.catch { handle(message) }
        .onLeft {
            SqsConnector.logger
                .atError()
                .addKeyValue("sqs.consumer", this::class)
                .setCause(it)
                .addKeyValue("sqs.message.id", message.id)
                .log(
                    "Consumer threw uncaught exception, message will be retried after $EXCEPTION_BACKOFF. " +
                        "To suppress this message, return MessageConsumer.Action.RetryBackoff instead.")
        }
        .getOrElse { RetryBackoff(message, EXCEPTION_BACKOFF) }

private suspend fun MessageConsumer.Batch.handleSafely(messages: List<Message<String>>) =
    Either.catch { handle(messages) }
        .onLeft {
            SqsConnector.logger
                .atError()
                .addKeyValue("sqs.consumer", this::class)
                .setCause(it)
                .addKeyValue("sqs.message.ids", messages.map(Message<*>::id))
                .log(
                    "Consumer threw uncaught exception, messages will be retried after $EXCEPTION_BACKOFF. " +
                        "To suppress this message, return MessageConsumer.Action.RetryBackoff instead.")
        }
        .getOrElse { messages.map { RetryBackoff(it, EXCEPTION_BACKOFF) } }

package io.github.jckoenen.flow

import arrow.core.Either
import arrow.core.getOrElse
import io.github.jckoenen.Message
import io.github.jckoenen.MessageConsumer
import io.github.jckoenen.MessageConsumer.Action.RetryBackoff
import io.github.jckoenen.SqsConnector
import io.github.jckoenen.impl.kotlin.SQS_BATCH_SIZE
import io.github.jckoenen.utils.chunked
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flatMapMerge
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map

private val CHUNK_TIMEOUT = 30.seconds
private val EXCEPTION_BACKOFF = 1.minutes

internal fun Flow<List<Message<String>>>.applyConsumer(consumer: MessageConsumer) =
    when (consumer) {
        is MessageConsumer.Individual ->
            flatMapMerge(consumer.configuration.parallelism, List<Message<String>>::asFlow)
                .map(consumer::handleSafely)
                .chunked(SQS_BATCH_SIZE, CHUNK_TIMEOUT)

        is MessageConsumer.Batch ->
            flatMapMerge(consumer.configuration.parallelism) { batch -> flow { emit(consumer.handleSafely(batch)) } }
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

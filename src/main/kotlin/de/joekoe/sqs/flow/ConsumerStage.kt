package de.joekoe.sqs.flow

import de.joekoe.sqs.Message
import de.joekoe.sqs.MessageConsumer
import de.joekoe.sqs.impl.kotlin.SQS_BATCH_SIZE
import de.joekoe.sqs.map
import de.joekoe.sqs.utils.chunked
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flatMapMerge
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map

private typealias ConsumerStage = FlowStage<Message<String>, MessageConsumer.Action>

internal fun MessageConsumer<*>.asStage(): ConsumerStage =
    when (this) {
        is MessageConsumer.Individual<*> ->
            FlowStage { upstream ->
                upstream
                    .flatMapMerge(configuration.parallelism, List<Message<String>>::asFlow)
                    .map(::consume)
                    .chunked(SQS_BATCH_SIZE, 30.seconds)
            }
        is MessageConsumer.Batch<*> ->
            FlowStage { upstream ->
                upstream.flatMapMerge(configuration.parallelism) { batch -> flowOf(consume(batch)) }
            }
    }

private suspend fun <T : Any> MessageConsumer.Individual<T>.consume(message: Message<String>) =
    handle(message.map(::parse))

private suspend fun <T : Any> MessageConsumer.Batch<T>.consume(messages: List<Message<String>>) =
    handle(messages.map { it.map(::parse) })

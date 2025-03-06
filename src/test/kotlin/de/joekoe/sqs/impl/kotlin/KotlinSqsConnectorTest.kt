package de.joekoe.sqs.impl.kotlin

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import de.joekoe.sqs.OutboundMessage
import de.joekoe.sqs.testinfra.SqsContainerExtension
import de.joekoe.sqs.testinfra.SqsContainerExtension.queueName
import de.joekoe.sqs.testinfra.assumeRight
import io.kotest.core.spec.style.FreeSpec
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotest.property.Arb
import io.kotest.property.RandomSource
import io.kotest.property.arbitrary.bind
import io.kotest.property.arbitrary.int
import io.kotest.property.arbitrary.string
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.takeWhile
import kotlinx.coroutines.flow.toList

class KotlinSqsConnectorTest : FreeSpec({
    "The kotlin implementation" - {
        val subject = SqsContainerExtension.newConnector()
        val count = 255
        val queue = subject.getOrCreateQueue(queueName()).assumeRight()

        "should produce and consume messages correctly" {
            val expected =
                Arb.string()
                    .generate(RandomSource.default())
                    .take(count)
                    .map { it.value }
                    .toList()

            subject.sendMessages(queue.url, expected.map(::OutboundMessage))
                .assumeRight()

            val actual =
                flow { while (true) emit(subject.receiveMessages(queue)) }
                    .buffer()
                    .map { it.assumeRight() }
                    .takeWhile { it.isNotEmpty() }
                    .onEach { batch ->
                        subject.deleteMessages(queue.url, batch.map { it.receiptHandle })
                            .assumeRight()
                    }
                    .buffer()
                    .flatMapConcat { it.asFlow() }
                    .map { it.content }
                    .take(count) // skip the last long poll when the test found all items
                    .toList()

            actual shouldContainExactlyInAnyOrder expected
        }
    }
})

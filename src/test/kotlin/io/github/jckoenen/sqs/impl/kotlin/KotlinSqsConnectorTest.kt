package io.github.jckoenen.sqs.impl.kotlin

import arrow.core.PotentiallyUnsafeNonEmptyOperation
import arrow.core.wrapAsNonEmptyListOrThrow
import io.github.jckoenen.sqs.Message
import io.github.jckoenen.sqs.OutboundMessage
import io.github.jckoenen.sqs.testinfra.SqsContainerExtension
import io.github.jckoenen.sqs.testinfra.SqsContainerExtension.queueName
import io.github.jckoenen.sqs.testinfra.assumeRight
import io.kotest.core.spec.style.FreeSpec
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.mapNotNull
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.takeWhile
import kotlinx.coroutines.flow.toList

@OptIn(PotentiallyUnsafeNonEmptyOperation::class)
class KotlinSqsConnectorTest : FreeSpec({
    "The kotlin implementation" - {
        val subject = SqsContainerExtension.newConnector()
        val count = 255
        val queue = subject.getOrCreateQueue(queueName()).assumeRight()

        "should produce and consume messages correctly" {
            val expected =
                generateSequence(0, Int::inc)
                    .take(count)
                    .map { "$it" to  Message.GroupId((it % 3).toString())}
                    .toList()
                    .wrapAsNonEmptyListOrThrow()

            subject.sendMessages(queue.url, expected.map { (body, group) -> OutboundMessage(body, groupId = group) })
                .assumeRight()

            val actual =
                flow { while (true) emit(subject.receiveMessages(queue)) }
                    .buffer()
                    .map { it.assumeRight() }
                    .takeWhile { it.isNotEmpty() }
                    .mapNotNull { it.wrapAsNonEmptyListOrThrow() }
                    .onEach { batch ->
                        subject.deleteMessages(queue.url, batch.map { it.receiptHandle })
                            .assumeRight()
                    }
                    .buffer()
                    .flatMapConcat { it.asFlow() }
                    .map { it.content to it.groupId }
                    .take(count) // skip the last long poll when the test found all items
                    .toList()

            actual shouldContainExactlyInAnyOrder expected
        }
    }
})

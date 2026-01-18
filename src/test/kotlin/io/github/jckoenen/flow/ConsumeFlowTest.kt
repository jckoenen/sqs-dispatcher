package io.github.jckoenen.flow

import arrow.core.PotentiallyUnsafeNonEmptyOperation
import arrow.core.wrapAsNonEmptyListOrThrow
import io.github.jckoenen.Message
import io.github.jckoenen.MessageConsumer.Action
import io.github.jckoenen.OutboundMessage
import io.github.jckoenen.testinfra.ProjectKotestConfiguration.Companion.eventually
import io.github.jckoenen.testinfra.SqsContainerExtension
import io.github.jckoenen.testinfra.SqsContainerExtension.queueName
import io.github.jckoenen.testinfra.TestMessageConsumer
import io.github.jckoenen.testinfra.assumeRight
import io.kotest.core.spec.style.FreeSpec
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.property.Arb
import io.kotest.property.RandomSource
import io.kotest.property.arbitrary.string
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import kotlin.time.Duration.Companion.seconds

@OptIn(PotentiallyUnsafeNonEmptyOperation::class)
class ConsumeFlowTest: FreeSpec ({
    "Using SqsConnector.consume" - {
        val connector = SqsContainerExtension.newConnector()
        val messageCount = 25
        val minimumVisibilityTimeout = 3.seconds

        "messages moved to dlq should not be received again" {
            val queue = connector.getOrCreateQueue(queueName(), createDlq = true)
                .assumeRight()
            val dlq = queue.dlq.shouldNotBeNull()

            val expected =
                generateSequence(0, Int::inc)
                    .take(messageCount)
                    .map { it.toString() }
                    .toList()
                    .wrapAsNonEmptyListOrThrow()

            connector.sendMessages(queue.url, expected.map { OutboundMessage(it) })
                .assumeRight()

            val dlqConsumer = TestMessageConsumer.create(handleFn = Action::DeleteMessage)
            val qConsumer = TestMessageConsumer.create(handleFn = Action::MoveMessageToDlq)
            qConsumer.seen
                .onEach { println("  Q Seen: ${it.map(Message<*>::content)}") }
                .launchIn(this)

            dlqConsumer.seen
                .onEach { println("DLQ Seen: ${it.map(Message<*>::content)}") }
                .launchIn(this)

            connector.consume(queue, qConsumer, visibilityTimeout = minimumVisibilityTimeout, automaticVisibilityExtension = null)
                .launchWithDrainControl(this)

            connector.consume(dlq, dlqConsumer, visibilityTimeout = minimumVisibilityTimeout, automaticVisibilityExtension = null)
                .launchWithDrainControl(this)

            eventually {
                val inDlq = dlqConsumer.seen.value.map(Message<String>::content)
                inDlq shouldContainExactlyInAnyOrder expected
            }

            delay(minimumVisibilityTimeout * 3)
            val seenInQueue = qConsumer.seen.value.map(Message<String>::content)
            seenInQueue shouldContainExactlyInAnyOrder expected

            cancel("Test complete")
        }
    }
}
)

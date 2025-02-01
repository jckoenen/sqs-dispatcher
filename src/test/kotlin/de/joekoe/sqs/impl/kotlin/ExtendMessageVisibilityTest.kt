package de.joekoe.sqs.impl.kotlin

import de.joekoe.sqs.OutboundMessage
import de.joekoe.sqs.impl.isMessageAlreadyDeleted
import de.joekoe.sqs.testinfra.SqsContainerExtension
import de.joekoe.sqs.testinfra.SqsContainerExtension.queueName
import io.kotest.core.spec.style.FreeSpec
import io.kotest.inspectors.forAll
import io.kotest.matchers.collections.beEmpty
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlin.time.Duration.Companion.seconds

class ExtendMessageVisibilityTest : FreeSpec({
    "Extending Message Visibility" - {
        val connector = SqsContainerExtension.newConnector()

        "should fail with expected error" {
            val queue = connector.getOrCreateQueue(queueName())
            val outbound = OutboundMessage(mapOf("foo" to "bar"))

            connector.sendMessages(queue, listOf(outbound)) should beEmpty()
            val received = flow { while (true) emit(connector.receiveMessages(queue)) }
                .flatMapConcat { it.asFlow() }
                .take(1)
                .map { it.receiptHandle }
                .toList()

            connector.deleteMessages(queue, received) should beEmpty()
            connector.extendMessageVisibility(queue, received, 5.seconds).forAll {
                it.isMessageAlreadyDeleted() shouldBe true
            }
        }
    }
})

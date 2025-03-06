package de.joekoe.sqs.impl.kotlin

import de.joekoe.sqs.OutboundMessage
import de.joekoe.sqs.SqsFailure
import de.joekoe.sqs.SqsFailure.ChangeMessagesFailure.MessageAlreadyDeleted
import de.joekoe.sqs.testinfra.SqsContainerExtension
import de.joekoe.sqs.testinfra.SqsContainerExtension.queueName
import de.joekoe.sqs.testinfra.assumeRight
import io.kotest.assertions.arrow.core.shouldBeLeft
import io.kotest.assertions.failureWithTypeInformation
import io.kotest.core.spec.style.FreeSpec
import io.kotest.inspectors.forAll
import io.kotest.matchers.collections.beEmpty
import io.kotest.matchers.collections.shouldHaveSingleElement
import io.kotest.matchers.maps.shouldHaveSize
import io.kotest.matchers.nulls.shouldNotBeNull
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
            val queue = connector.getOrCreateQueue(queueName()).assumeRight()
            val outbound = OutboundMessage(mapOf("foo" to "bar"))

            connector.sendMessages(queue.url, listOf(outbound)).assumeRight()
            val received = flow { while (true) emit(connector.receiveMessages(queue)) }
                .flatMapConcat { it.assumeRight().asFlow() }
                .take(1)
                .map { it.receiptHandle }
                .toList()

            connector.deleteMessages(queue.url, received).assumeRight()
            val failures = connector.extendMessageVisibility(queue.url, received, 5.seconds)
                .leftOrNull()

            failures.shouldNotBeNull()
            failures shouldHaveSize 1
            failures.keys shouldHaveSingleElement { it is MessageAlreadyDeleted }
        }
    }
})

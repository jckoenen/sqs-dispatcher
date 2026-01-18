package io.github.jckoenen.impl.kotlin

import arrow.core.PotentiallyUnsafeNonEmptyOperation
import arrow.core.nonEmptyListOf
import arrow.core.toNonEmptyListOrThrow
import arrow.core.wrapAsNonEmptyListOrThrow
import io.github.jckoenen.OutboundMessage
import io.github.jckoenen.SqsFailure.ChangeMessagesFailure.MessageAlreadyDeleted
import io.github.jckoenen.testinfra.SqsContainerExtension
import io.github.jckoenen.testinfra.SqsContainerExtension.queueName
import io.github.jckoenen.testinfra.assumeRight
import io.kotest.core.spec.style.FreeSpec
import io.kotest.matchers.collections.shouldHaveSingleElement
import io.kotest.matchers.maps.shouldHaveSize
import io.kotest.matchers.nulls.shouldNotBeNull
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlin.time.Duration.Companion.seconds

@OptIn(PotentiallyUnsafeNonEmptyOperation::class)
class ExtendMessageVisibilityTest : FreeSpec({
    "Extending Message Visibility" - {
        val connector = SqsContainerExtension.newConnector()

        "should fail with expected error" {
            val queue = connector.getOrCreateQueue(queueName()).assumeRight()
            val outbound = OutboundMessage("hello, world")

            connector.sendMessages(queue.url, nonEmptyListOf(outbound)).assumeRight()
            val received = flow { while (true) emit(connector.receiveMessages(queue)) }
                .flatMapConcat { it.assumeRight().asFlow() }
                .take(1)
                .map { it.receiptHandle }
                .toList()
                .wrapAsNonEmptyListOrThrow()

            connector.deleteMessages(queue.url, received).assumeRight()
            val failures = connector.extendMessageVisibility(queue.url, received, 5.seconds)
                .leftOrNull()

            failures.shouldNotBeNull()
            failures shouldHaveSize 1
            failures.keys shouldHaveSingleElement { it is MessageAlreadyDeleted }
        }
    }
})

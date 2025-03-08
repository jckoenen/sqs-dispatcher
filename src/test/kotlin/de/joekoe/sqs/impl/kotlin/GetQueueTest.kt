package de.joekoe.sqs.impl.kotlin

import de.joekoe.sqs.Queue
import de.joekoe.sqs.SqsFailure
import de.joekoe.sqs.impl.shouldDenoteSameQueueAs
import de.joekoe.sqs.testinfra.SqsContainerExtension
import de.joekoe.sqs.testinfra.SqsContainerExtension.fifoQueueName
import de.joekoe.sqs.testinfra.SqsContainerExtension.queueName
import de.joekoe.sqs.testinfra.assumeRight
import de.joekoe.sqs.testinfra.right
import io.kotest.assertions.arrow.core.shouldBeLeft
import io.kotest.assertions.arrow.core.shouldBeRight
import io.kotest.core.spec.style.FreeSpec
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf

class GetQueueTest: FreeSpec({
    "Get Queue should" - {
        val subject = SqsContainerExtension.newConnector()
        "return Left(QueueDoesNotExist) if queue does not exist" {
            subject.getQueue(queueName()).shouldBeLeft().shouldBeInstanceOf<SqsFailure.QueueDoesNotExist>()
        }
        "return an existing regular queue" - {
            "with dlq" {
                val existing = subject.getOrCreateQueue(queueName(), createDlq = true).assumeRight()

                subject.getQueue(existing.name) shouldBe right<Queue> {
                    name shouldBe existing.name
                    url shouldDenoteSameQueueAs existing.url
                    dlqUrl shouldDenoteSameQueueAs existing.dlqUrl
                }
            }
            "without dlq" {
                val existing = subject.getOrCreateQueue(queueName(), createDlq = false).assumeRight()

                subject.getQueue(existing.name) shouldBe right<Queue> {
                    name shouldBe existing.name
                    url shouldDenoteSameQueueAs existing.url
                    dlqUrl.shouldBeNull()
                }
            }
        }
        "return an existing FIFO queue" - {
            "with FIFO dlq" {
                val existing = subject.getOrCreateQueue(fifoQueueName(), createDlq = true).assumeRight()

                subject.getQueue(existing.name) shouldBe right<Queue> {
                    name shouldBe existing.name
                    url shouldDenoteSameQueueAs existing.url
                    dlqUrl shouldDenoteSameQueueAs existing.dlqUrl
                }
            }
            "without dlq" {
                val existing = subject.getOrCreateQueue(fifoQueueName(), createDlq = false).assumeRight()

                subject.getQueue(existing.name) shouldBe right<Queue> {
                    name shouldBe existing.name
                    url shouldDenoteSameQueueAs existing.url
                    dlqUrl.shouldBeNull()
                }
            }
        }
    }
})

package io.github.jckoenen.sqs.impl.kotlin

import io.github.jckoenen.sqs.Queue
import io.github.jckoenen.sqs.SqsFailure
import io.github.jckoenen.sqs.impl.shouldDenoteSameQueueAs
import io.github.jckoenen.sqs.testinfra.SqsContainerExtension
import io.github.jckoenen.sqs.testinfra.SqsContainerExtension.fifoQueueName
import io.github.jckoenen.sqs.testinfra.SqsContainerExtension.queueName
import io.github.jckoenen.sqs.testinfra.assumeRight
import io.github.jckoenen.sqs.testinfra.right
import io.kotest.assertions.arrow.core.shouldBeLeft
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
                    dlq?.url shouldDenoteSameQueueAs existing.dlq?.url
                }
            }
            "without dlq" {
                val existing = subject.getOrCreateQueue(queueName(), createDlq = false).assumeRight()

                subject.getQueue(existing.name) shouldBe right<Queue> {
                    name shouldBe existing.name
                    url shouldDenoteSameQueueAs existing.url
                    dlq.shouldBeNull()
                }
            }
        }
        "return an existing FIFO queue" - {
            "with FIFO dlq" {
                val existing = subject.getOrCreateQueue(fifoQueueName(), createDlq = true).assumeRight()

                subject.getQueue(existing.name) shouldBe right<Queue> {
                    name shouldBe existing.name
                    url shouldDenoteSameQueueAs existing.url
                    dlq?.url shouldDenoteSameQueueAs existing.dlq?.url
                }
            }
            "without dlq" {
                val existing = subject.getOrCreateQueue(fifoQueueName(), createDlq = false).assumeRight()

                subject.getQueue(existing.name) shouldBe right<Queue> {
                    name shouldBe existing.name
                    url shouldDenoteSameQueueAs existing.url
                    dlq.shouldBeNull()
                }
            }
        }
    }
})

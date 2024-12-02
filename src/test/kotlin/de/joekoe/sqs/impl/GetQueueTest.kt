package de.joekoe.sqs.impl

import de.joekoe.sqs.testinfra.SqsContainerExtension
import de.joekoe.sqs.testinfra.SqsContainerExtension.fifoQueueName
import de.joekoe.sqs.testinfra.SqsContainerExtension.queueName
import io.kotest.core.spec.style.FreeSpec
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.shouldBe

class GetQueueTest: FreeSpec({
    "Get Queue should" - {
        val subject = SqsContainerExtension.newConnector()
        "return null if queue does not exist" {
            subject.getQueue(queueName()) shouldBe null
        }
        "return an existing regular queue" - {
            "with dlq" {
                val existing = subject.getOrCreateQueue(queueName(), createDlq = true)
                val actual = subject.getQueue(existing.name)

                actual?.name shouldBe existing.name
                actual?.url shouldDenoteSameQueueAs  existing.url
                actual?.visibilityTimeout shouldBe existing.visibilityTimeout
                actual?.dlqUrl shouldDenoteSameQueueAs existing.dlqUrl
            }
            "without dlq" {
                val existing = subject.getOrCreateQueue(queueName(), createDlq = false)
                val actual = subject.getQueue(existing.name)

                actual?.name shouldBe existing.name
                actual?.url shouldDenoteSameQueueAs  existing.url
                actual?.visibilityTimeout shouldBe existing.visibilityTimeout
                actual?.dlqUrl.shouldBeNull()
            }
        }
        "return an existing FIFO queue" - {
            "with FIFO dlq" {
                val existing = subject.getOrCreateQueue(fifoQueueName(), createDlq = true)
                val actual = subject.getQueue(existing.name)

                actual?.name shouldBe existing.name
                actual?.url shouldDenoteSameQueueAs  existing.url
                actual?.visibilityTimeout shouldBe existing.visibilityTimeout
                actual?.dlqUrl shouldDenoteSameQueueAs existing.dlqUrl
            }
            "without dlq" {
                val existing = subject.getOrCreateQueue(fifoQueueName(), createDlq = false)
                val actual = subject.getQueue(existing.name)

                actual?.name shouldBe existing.name
                actual?.url shouldDenoteSameQueueAs  existing.url
                actual?.visibilityTimeout shouldBe existing.visibilityTimeout
                actual?.dlqUrl.shouldBeNull()
            }
        }
    }
})

package de.joekoe.sqs.impl.kotlin

import de.joekoe.sqs.Queue
import de.joekoe.sqs.impl.shouldDenoteSameQueueAs
import de.joekoe.sqs.testinfra.SqsContainerExtension
import de.joekoe.sqs.testinfra.SqsContainerExtension.queueName
import de.joekoe.sqs.testinfra.assumeRight
import de.joekoe.sqs.testinfra.right
import io.kotest.assertions.arrow.core.shouldBeRight
import io.kotest.core.spec.style.FreeSpec
import io.kotest.matchers.nulls.beNull
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNot

class CreateQueueTest : FreeSpec({
    "Get or Create Queue should" - {
        val subject = SqsContainerExtension.newConnector()

        "create queues successfully" - {
            "without dlq" {
                val expect = queueName()

                subject.getOrCreateQueue(expect, createDlq = false) shouldBe right<Queue> {
                    dlqUrl should beNull()
                    name shouldBe expect
                }
            }
            "with dlq" {
                val expect = queueName()

                subject.getOrCreateQueue(queueName(), createDlq = true) shouldBe right<Queue> {
                    dlqUrl shouldNot beNull()
                    name shouldBe expect
                }
            }
        }
        "return existing queues" - {
            "with existing dlq" {
                val existing = subject.getOrCreateQueue(queueName(), createDlq = true).assumeRight()

                subject.getOrCreateQueue(existing.name, createDlq = true) shouldBe right<Queue> {
                    name shouldBe existing.name
                    url shouldBe existing.url
                    dlqUrl shouldDenoteSameQueueAs existing.dlqUrl
                }
            }
            "with existing dlq even though createdDlq = false" {
                val existing = subject.getOrCreateQueue(queueName(), createDlq = true).assumeRight()

                subject.getOrCreateQueue(existing.name, createDlq = false) shouldBe right<Queue> {
                    name shouldBe existing.name
                    url shouldBe existing.url
                    dlqUrl shouldDenoteSameQueueAs existing.dlqUrl
                }

            }
            "with newly created dlq" {
                val existing = subject.getOrCreateQueue(queueName(), createDlq = false).assumeRight()

                subject.getOrCreateQueue(existing.name, createDlq = true) shouldBe right<Queue> {
                    name shouldBe existing.name
                    url shouldBe existing.url
                    dlqUrl.shouldNotBeNull()
                }
            }
            "without existing dlq" {
                val existing = subject.getOrCreateQueue(queueName(), createDlq = false).assumeRight()

                subject.getOrCreateQueue(existing.name, createDlq = false) shouldBeRight existing
            }
        }
    }
})

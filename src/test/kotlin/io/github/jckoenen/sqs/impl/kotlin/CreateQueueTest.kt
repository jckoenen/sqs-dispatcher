package io.github.jckoenen.sqs.impl.kotlin

import io.github.jckoenen.sqs.Queue
import io.github.jckoenen.sqs.impl.shouldDenoteSameQueueAs
import io.github.jckoenen.sqs.testinfra.SqsContainerExtension
import io.github.jckoenen.sqs.testinfra.SqsContainerExtension.queueName
import io.github.jckoenen.sqs.testinfra.assumeRight
import io.github.jckoenen.sqs.testinfra.right
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
                    dlq should beNull()
                    name shouldBe expect
                }
            }
            "with dlq" {
                val expect = queueName()

                subject.getOrCreateQueue(queueName(), createDlq = true) shouldBe right<Queue> {
                    dlq shouldNot beNull()
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
                    dlq?.url shouldDenoteSameQueueAs existing.dlq?.url
                }
            }
            "with existing dlq even though createdDlq = false" {
                val existing = subject.getOrCreateQueue(queueName(), createDlq = true).assumeRight()

                subject.getOrCreateQueue(existing.name, createDlq = false) shouldBe right<Queue> {
                    name shouldBe existing.name
                    url shouldBe existing.url
                    dlq?.url shouldDenoteSameQueueAs existing.dlq?.url
                }

            }
            "with newly created dlq" {
                val existing = subject.getOrCreateQueue(queueName(), createDlq = false).assumeRight()

                subject.getOrCreateQueue(existing.name, createDlq = true) shouldBe right<Queue> {
                    name shouldBe existing.name
                    url shouldBe existing.url
                    dlq.shouldNotBeNull()
                }
            }
            "without existing dlq" {
                val existing = subject.getOrCreateQueue(queueName(), createDlq = false).assumeRight()

                subject.getOrCreateQueue(existing.name, createDlq = false) shouldBeRight existing
            }
        }
    }
})

package de.joekoe.sqs.impl

import aws.smithy.kotlin.runtime.net.url.Url
import de.joekoe.sqs.Queue
import de.joekoe.sqs.testinfra.SqsContainerExtension
import de.joekoe.sqs.testinfra.SqsContainerExtension.queueName
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
                val actual = subject.getOrCreateQueue(expect, createDlq = false)

                actual.dlqUrl should beNull()
                actual.name shouldBe expect
            }
            "with dlq" {
                val expect = queueName()
                val actual = subject.getOrCreateQueue(queueName(), createDlq = true)

                actual.dlqUrl shouldNot beNull()
                actual.name shouldBe expect
            }
        }
        "return existing queues" - {
            "with existing dlq" {
                val existing = subject.getOrCreateQueue(queueName(), createDlq = true)
                val created = subject.getOrCreateQueue(existing.name, createDlq = true)

                created.name shouldBe existing.name
                created.url shouldBe existing.url
                created.dlqUrl?.accountSegment shouldBe existing.dlqUrl?.accountSegment
                created.dlqUrl?.nameSegment shouldBe existing.dlqUrl?.nameSegment
            }
            "with existing dlq even though createdDlq = false" {
                val existing = subject.getOrCreateQueue(queueName(), createDlq = true)
                val created = subject.getOrCreateQueue(existing.name, createDlq = false)

                created.name shouldBe existing.name
                created.url shouldBe existing.url
                created.dlqUrl?.accountSegment shouldBe existing.dlqUrl?.accountSegment
                created.dlqUrl?.nameSegment shouldBe existing.dlqUrl?.nameSegment
            }
            "with newly created dlq" {
                val existing = subject.getOrCreateQueue(queueName(), createDlq = false)
                val created = subject.getOrCreateQueue(existing.name, createDlq = true)

                created.name shouldBe existing.name
                created.url shouldBe existing.url
                created.dlqUrl.shouldNotBeNull()
            }
            "without existing dlq" {
                val existing = subject.getOrCreateQueue(queueName(), createDlq = false)
                val created = subject.getOrCreateQueue(existing.name, createDlq = false)

                created shouldBe existing
            }
        }
    }
})

private val Queue.Url.accountSegment: String
    get() = Url.parse(value).path.segments.last().decoded

private val Queue.Url.nameSegment: String
    get() = Url.parse(value).path.segments.dropLast(1).last().decoded

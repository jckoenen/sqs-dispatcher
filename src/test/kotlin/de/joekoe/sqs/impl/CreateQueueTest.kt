package de.joekoe.sqs.impl

import de.joekoe.sqs.testinfra.SqsContainerExtension
import de.joekoe.sqs.testinfra.SqsContainerExtension.queueName
import io.kotest.assertions.throwables.shouldNotThrowAny
import io.kotest.core.spec.style.FreeSpec
import io.kotest.matchers.nulls.beNull
import io.kotest.matchers.should
import io.kotest.matchers.shouldNot

class CreateQueueTest : FreeSpec({
    "Create Queue should" - {
        val subject = SqsContainerExtension.newConnector()

        "create queues successfully" - {
            "without dlq" {
                val actual = shouldNotThrowAny {
                    subject.createQueue(queueName(), false)
                }

                actual.dlqUrl should beNull()
            }

            "with dlq" {
                val actual = shouldNotThrowAny {
                    subject.createQueue(queueName(), true)
                }

                actual.dlqUrl shouldNot beNull()
            }
        }

    }
})

package io.github.jckoenen.sqs.impl

import io.github.jckoenen.sqs.Queue
import io.github.jckoenen.sqs.testinfra.assumeRight
import io.github.jckoenen.sqs.testinfra.beRight
import io.kotest.assertions.arrow.core.shouldBeLeft
import io.kotest.core.spec.style.FreeSpec
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe

class QueueArnTest : FreeSpec({
    "Parsing from" - {
        "Queue.Url" - {
            "should work" - {
                "for real urls" {
                    QueueArn.fromUrl(Queue.Url("https://sqs.eu-west-1.amazonaws.com/111111111111/some-queue.fifo")) should beRight {
                        name shouldBe Queue.Name("some-queue.fifo")
                        region shouldBe "eu-west-1"
                        accountId shouldBe "111111111111"
                    }

                }
                "for localstack urls" {
                    QueueArn.fromUrl(Queue.Url("http://sqs.us-east-1.localhost:4566/000000000000/DLQ-some-queue")) should beRight {
                        name shouldBe Queue.Name("DLQ-some-queue")
                        region shouldBe "us-east-1"
                        accountId shouldBe "000000000000"
                    }
                }
            }
            "should fail" - {
                "for non sqs urls" {
                    QueueArn.fromUrl(Queue.Url("http://sns.us-east-1.localhost:4566/000000000000/DLQ-some-topic"))
                        .shouldBeLeft()
                }
                "for urls without account" {
                    QueueArn.fromUrl(Queue.Url("http://sqs.us-east-1.localhost:4566/DLQ-some-queue"))
                        .shouldBeLeft()
                }
                "for urls without name" {
                    QueueArn.fromUrl(Queue.Url("http://sqs.us-east-1.localhost:4566/000000000000"))
                        .shouldBeLeft()
                }
                "for other urls" {
                    QueueArn.fromUrl(Queue.Url("http://sqs")).shouldBeLeft()
                }
            }
        }
        "kotlin.String" - {
            "should work" - {
                "for real ARNs" {
                    QueueArn.fromString("arn:aws:sqs:eu-west-1:111111111111:some-queue.fifo") should beRight {
                        name shouldBe Queue.Name("some-queue.fifo")
                        region shouldBe "eu-west-1"
                        accountId shouldBe "111111111111"
                    }
                }
            }
            "should fail" - {
                "for malformed ARNs" {
                    QueueArn.fromString("not an ARN lol").shouldBeLeft()
                }
                "for non sqs ARNs" {
                    QueueArn.fromString("arn:aws:sns:eu-west-1:000000000000:some-topic").shouldBeLeft()
                }
            }
        }
    }

    "toString should produce equal arn" {
        val source = "arn:aws:sqs:eu-west-1:111111111111:some-queue.fifo"
        val arn = QueueArn.fromString(source).assumeRight()

        arn.toString() shouldBe source
    }
})

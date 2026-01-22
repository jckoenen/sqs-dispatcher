package io.github.jckoenen.sqs.impl

import io.github.jckoenen.sqs.Queue
import io.kotest.core.spec.style.FreeSpec
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe

class QueueArnTest : FreeSpec({
    "Parsing from" - {
        "Queue.Url" - {
            "should work" - {
                "for real urls" {
                    val actual = QueueArn.fromUrl(Queue.Url("https://sqs.eu-west-1.amazonaws.com/111111111111/some-queue.fifo"))

                    actual.shouldNotBeNull()
                    actual.name shouldBe Queue.Name("some-queue.fifo")
                    actual.region shouldBe "eu-west-1"
                    actual.accountId shouldBe "111111111111"
                }
                "for localstack urls" {
                    val actual = QueueArn.fromUrl(Queue.Url("http://sqs.us-east-1.localhost:4566/000000000000/DLQ-some-queue"))

                    actual.shouldNotBeNull()
                    actual.name shouldBe Queue.Name("DLQ-some-queue")
                    actual.region shouldBe "us-east-1"
                    actual.accountId shouldBe "000000000000"
                }
            }
            "should fail" - {
                "for non sqs urls" {
                    QueueArn.fromUrl(Queue.Url("http://sns.us-east-1.localhost:4566/000000000000/DLQ-some-topic"))
                        .shouldBeNull()
                }
                "for urls without account" {
                    QueueArn.fromUrl(Queue.Url("http://sqs.us-east-1.localhost:4566/DLQ-some-queue"))
                        .shouldBeNull()
                }
                "for urls without name" {
                    QueueArn.fromUrl(Queue.Url("http://sqs.us-east-1.localhost:4566/000000000000"))
                        .shouldBeNull()
                }
                "for other urls" {
                    QueueArn.fromUrl(Queue.Url("http://sqs")).shouldBeNull()
                }
            }
        }
        "kotlin.String" - {
            "should work" - {
                "for localstack ARNs" {

                }
                "for real ARNs" {
                    val actual = QueueArn.fromString("arn:aws:sqs:eu-west-1:111111111111:some-queue.fifo")

                    actual.shouldNotBeNull()
                    actual.name shouldBe Queue.Name("some-queue.fifo")
                    actual.region shouldBe "eu-west-1"
                    actual.accountId shouldBe "111111111111"
                }
            }
            "should fail" - {
                "for malformed ARNs" {
                    QueueArn.fromString("not an ARN lol").shouldBeNull()
                }
                "for non sqs ARNs" {
                    QueueArn.fromString("arn:aws:sns:eu-west-1:000000000000:some-topic").shouldBeNull()
                }
            }
        }
    }

    "toString should produce equal arn" {
        val source = "arn:aws:sqs:eu-west-1:111111111111:some-queue.fifo"
        val arn = QueueArn.fromString(source).shouldNotBeNull()

        arn.toString() shouldBe source
    }
})

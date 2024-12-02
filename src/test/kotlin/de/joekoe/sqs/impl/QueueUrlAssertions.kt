package de.joekoe.sqs.impl

import aws.smithy.kotlin.runtime.net.url.Url
import de.joekoe.sqs.Queue
import io.kotest.assertions.assertSoftly
import io.kotest.matchers.shouldBe

private val Queue.Url.accountSegment: String
    get() = Url.parse(value).path.segments.last().decoded

private val Queue.Url.nameSegment: String
    get() = Url.parse(value).path.segments.dropLast(1).last().decoded

internal infix fun Queue.Url?.shouldDenoteSameQueueAs(expected: Queue.Url?) =
    assertSoftly(this) {
        this?.accountSegment shouldBe expected?.accountSegment
        this?.nameSegment shouldBe expected?.nameSegment
    }

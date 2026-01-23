package io.github.jckoenen.sqs.flow

import io.kotest.core.spec.style.FreeSpec
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onEach

class DrainableTest : FreeSpec({
    "Flow.drainable" - {
        fun newUpstream(): Pair<Channel<String>, Flow<Int>> {
            val signals = Channel<String>(capacity = Channel.BUFFERED)
            val upstream = flow {
                signals.send("START")
                emit(0)
                try {
                    awaitCancellation()
                } finally {
                    signals.send("CANCELLED")
                }
            }
            return signals to upstream
        }

        "should propagate cancellation to upstream" - {
            val (signals, upstream) = newUpstream()
            val control = upstream
                .drainable()
                .launchWithDrainControl(this)

            signals.receive() shouldBe "START"
            control.job.cancel()
            signals.receive() shouldBe "CANCELLED"

            control.job.join()
            control.job.isCompleted shouldBe true
            control.job.isCancelled shouldBe true
        }

        "should cancel upstream but complete downstream on drain" - {
            val (signals, upstream) = newUpstream()
            val control = upstream
                .drainable()
                .launchWithDrainControl(this)

            signals.receive() shouldBe "START"
            control.drain()
            signals.receive() shouldBe "CANCELLED"

            control.job.join()
            control.job.isCompleted shouldBe true
            control.job.isCancelled shouldBe false
        }

        "chaining should only cancel first upstream" - {
            val seen = mutableListOf<Int>()
            val (signals, upstream) = newUpstream()

            val control = upstream
                .drainable()
                .flatMapConcat {
                    flow {
                        emit(it)
                        emit(2)
                    }
                }
                .drainable()
                .flatMapConcat {
                    flow {
                        emit(it)
                        emit(1)
                    }
                }
                .onEach(seen::add)
                .drainable()
                .launchWithDrainControl(this)

            signals.receive() shouldBe "START"
            control.drain()
            signals.receive() shouldBe "CANCELLED"

            control.job.join()
            control.job.isCompleted shouldBe true
            control.job.isCancelled shouldBe false

            seen shouldContainExactly listOf(0, 1, 2, 1)
        }

        "collecting multiple times should be independent" {
            val (signals, source) = newUpstream()
            val upstream = source.drainable()

            val controlA = upstream.launchWithDrainControl(this)
            val controlB = upstream.launchWithDrainControl(this)

            signals.receive() shouldBe "START"
            signals.receive() shouldBe "START"

            controlA.drainAndJoin()
            signals.receive() shouldBe "CANCELLED"

            controlB.job.isActive shouldBe true
            signals.tryReceive().isFailure shouldBe true // = channel is empty

            controlB.drainAndJoin()
            signals.receive() shouldBe "CANCELLED"
        }
    }
})

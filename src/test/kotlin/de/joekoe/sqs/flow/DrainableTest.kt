package de.joekoe.sqs.flow

import io.kotest.core.spec.style.FreeSpec
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow

class DrainableTest : FreeSpec({
    "Flow.drainable" - {
        fun newUpstream(): Pair<Channel<Int>, Flow<Unit>> {
            val signals = Channel<Int>(capacity = Channel.BUFFERED)
            val upstream = flow<Unit> {
                signals.send(0)
                try {
                    awaitCancellation()
                } finally {
                    signals.send(1)
                }
            }
            return signals to upstream
        }

        "should propagate cancellation to upstream" - {
            val (signals, upstream) = newUpstream()
            val control = upstream
                .drainable()
                .launchWithDrainControl(this)

            signals.receive() shouldBe 0
            control.job.cancel()
            signals.receive() shouldBe 1

            control.job.join()
            control.job.isCompleted shouldBe true
            control.job.isCancelled shouldBe true
        }
        "should cancel upstream but complete downstream on drain" - {
            val (signals, upstream) = newUpstream()
            val control = upstream
                .drainable()
                .launchWithDrainControl(this)

            signals.receive() shouldBe 0
            control.drain()
            signals.receive() shouldBe 1

            control.job.join()
            control.job.isCompleted shouldBe true
            control.job.isCancelled shouldBe false
        }
    }
})

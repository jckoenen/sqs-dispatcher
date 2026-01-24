package io.github.jckoenen.sqs.impl

import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient

@Serializable
internal data class RedrivePolicy(val maxReceiveCount: Int, val deadLetterTargetArn: String) {
    @Transient val targetArn = QueueArn.fromString(deadLetterTargetArn)
}

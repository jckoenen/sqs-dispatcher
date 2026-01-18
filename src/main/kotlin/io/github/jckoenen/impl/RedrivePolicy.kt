package io.github.jckoenen.impl

import com.fasterxml.jackson.annotation.JsonIgnore

internal data class RedrivePolicy(val maxReceiveCount: Int, val deadLetterTargetArn: String) {
    @JsonIgnore val targetArn = QueueArn.fromString(deadLetterTargetArn)
}

package io.github.jckoenen

public interface Failure {
    public val customTags: Map<String, Any>
    public val message: String
}

internal fun Failure.allTags() = customTags + mapOf("sqs.failure.kind" to (this::class.simpleName ?: "Unnamed failure"))

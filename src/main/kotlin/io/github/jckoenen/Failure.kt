package io.github.jckoenen

interface Failure {
    val customTags: Map<String, Any>
    val message: String
}

fun Failure.allTags() = customTags + mapOf("failure.kind" to (this::class.simpleName ?: "Unnamed failure"))

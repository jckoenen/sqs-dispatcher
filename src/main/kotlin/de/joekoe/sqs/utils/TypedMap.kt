package de.joekoe.sqs.utils

import kotlin.reflect.KClass

private typealias ClassMap<T> = Map<KClass<out T>, List<T>>

@JvmInline
internal value class TypedMap<T : Any> private constructor(private val underlying: ClassMap<T>) :
    ClassMap<T> by underlying {

    @Suppress("UNCHECKED_CAST") inline fun <reified K : T> get() = underlying[K::class] as List<K>

    companion object {
        fun <T : Any> Collection<T>.byType() = TypedMap(groupBy { it::class })
    }
}

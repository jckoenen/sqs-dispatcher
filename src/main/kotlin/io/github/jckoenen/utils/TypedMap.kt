package io.github.jckoenen.utils

import arrow.core.Nel
import arrow.core.toNonEmptyListOrNull
import kotlin.experimental.ExperimentalTypeInference
import kotlin.reflect.KClass

private typealias ClassMap<T> = Map<KClass<out T>, List<T>>

@JvmInline
internal value class TypedMap<T : Any> private constructor(private val underlying: ClassMap<T>) :
    ClassMap<T> by underlying {

    @Suppress("UNCHECKED_CAST")
    inline fun <reified K : T> get(): Nel<K>? = underlying[K::class]?.toNonEmptyListOrNull() as? Nel<K>

    @OptIn(ExperimentalTypeInference::class)
    @BuilderInference
    inline fun <reified K : T, R> onMatching(f: (Nel<K>) -> R): R? = get<K>()?.let(f)

    companion object {
        fun <T : Any> Collection<T>.byType() = TypedMap(groupBy { it::class })
    }
}

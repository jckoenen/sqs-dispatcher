package io.github.jckoenen.sqs.utils

import arrow.core.fold
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.slf4j.MDCContext
import org.slf4j.MDC
import org.slf4j.spi.LoggingEventBuilder

internal fun LoggingEventBuilder.putAll(map: Map<String, Any>) = map.fold(this) { e, (k, v) -> e.addKeyValue(k, v) }

internal fun mdc(map: Map<String, String>): CoroutineContext {
    val src = MDC.getCopyOfContextMap().orEmpty()
    val ctx = src + map
    return MDCContext(ctx)
}

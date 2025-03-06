package de.joekoe.sqs.utils

import arrow.core.fold
import org.slf4j.spi.LoggingEventBuilder

internal fun LoggingEventBuilder.putAll(map: Map<String, Any>) = map.fold(this) { e, (k, v) -> e.addKeyValue(k, v) }

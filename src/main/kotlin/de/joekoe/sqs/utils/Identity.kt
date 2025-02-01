package de.joekoe.sqs.utils

@OptIn(ExperimentalStdlibApi::class) internal fun Any.identityCode() = System.identityHashCode(this).toHexString()

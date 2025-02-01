package de.joekoe.sqs.impl

import de.joekoe.sqs.SqsConnector

internal fun SqsConnector.FailedBatchEntry<*>.isMessageAlreadyDeleted() =
    !senderFault && code == "InvalidParameterValueException"

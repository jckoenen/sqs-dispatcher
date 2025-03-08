package de.joekoe.sqs.flow

import de.joekoe.sqs.Message
import de.joekoe.sqs.MessageBound
import de.joekoe.sqs.Queue
import de.joekoe.sqs.SqsConnector
import de.joekoe.sqs.SqsFailure.ChangeMessagesFailure.MessageAlreadyDeleted
import de.joekoe.sqs.allTags
import de.joekoe.sqs.utils.asTags
import de.joekoe.sqs.utils.id
import de.joekoe.sqs.utils.putAll
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory

class VisibilityExtensionStage<A : MessageBound>(
    connector: SqsConnector,
    extensionDuration: Duration,
    scope: CoroutineScope,
) : FlowStage<A, A> {
    private val manager = VisibilityManager(connector, extensionDuration, scope)

    override fun inbound(upstream: Flow<List<A>>): Flow<List<A>> = upstream.onEach(manager::startTracking)

    override fun <C : MessageBound> outbound(upstream: Flow<List<C>>): Flow<List<C>> =
        upstream.onEach { batch -> batch.forEach { manager.stopTracking(it) } }
}

private class VisibilityManager(
    private val connector: SqsConnector,
    private val extensionDuration: Duration,
    scope: CoroutineScope,
    private val extensionThreshold: Duration = 3.seconds,
) : CoroutineScope by scope {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val activeBatches = BatchMap<Message.ReceiptHandle>()

    suspend fun startTracking(messages: List<MessageBound>) {
        messages.groupBy(MessageBound::queue).forEach { (queue, byQueue) ->
            val ref = activeBatches.register(byQueue.map(MessageBound::receiptHandle))
            val interval = extensionDuration - extensionThreshold
            schedule(interval, queue, ref)

            logger
                .atDebug()
                .addKeyValue("visibilityBatch.id", ref.identityCode())
                .addKeyValue("visibilityBatch.size", byQueue.size)
                .addKeyValue("refresh.interval", interval.toString())
                .addKeyValue("refresh.duration", extensionDuration.toString())
                .putAll(queue.id().asTags())
                .log("Started automatic visibility management")
        }
    }

    suspend fun stopTracking(message: MessageBound) {
        activeBatches.remove(message.receiptHandle)
    }

    private fun schedule(delay: Duration, queue: Queue, reference: BatchMap.BatchRef<Message.ReceiptHandle>): Job =
        launch {
            delay(delay)
            val messages = reference.items()
            val log =
                logger
                    .atDebug()
                    .addKeyValue("visibilityBatch.id", messages.identityCode())
                    .addKeyValue("visibilityBatch.size", messages.size)

            if (messages.isEmpty()) {
                log.log("No messages left to extend visibility")
                return@launch
            }
            log.log("Extending visibility")

            connector
                .extendMessageVisibility(queue.url, messages, extensionDuration)
                .leftOrNull()
                .orEmpty()
                .flatMap { (cause, affected) -> affected.map { cause to it } }
                .onEach { (cause, failure) ->
                    if (cause is MessageAlreadyDeleted) {
                        logger
                            .atDebug()
                            .addKeyValue("failure.ref", failure.reference)
                            .log("Message was already deleted, ignoring")
                    } else {
                        logger
                            .atWarn()
                            .putAll(cause.allTags())
                            .addKeyValue("failure.ref", failure.reference)
                            .addKeyValue("failure.code", failure.code)
                            .addKeyValue("failure.message", failure.errorMessage)
                            .addKeyValue("failure.senderFault", failure.senderFault)
                            .log("Couldn't extend visibility for message. Will NOT retry")
                    }
                }
                .forEach { (_, failure) -> activeBatches.remove(failure.reference) }

            schedule(extensionDuration - extensionThreshold, queue, reference)
        }

    private data class BatchMap<T>(private val batches: MutableMap<T, MutableSet<T>> = mutableMapOf()) {
        /**
         * Prevents concurrent modification of [batches]
         *
         * There are three concurrent actors operating on it:
         * 1. Upstream emitting a new batch, registering items using [register]
         * 2. Downstream emitting an item, causing un-registration using [remove]
         * 3. The manager itself after an item failed to be extended, also using [remove]
         */
        private val mutex = Mutex()

        /**
         * Enforces batch registration to go through the mutex and associates it with that same mutex
         *
         * The items reference in the ref is itself mutable, meaning a call to [remove] will remove it from our global
         * [batches] map, but also from the individual ref it was contained in
         */
        suspend fun register(batch: Collection<T>): BatchRef<T> =
            mutex.withLock {
                val inner = batch.toMutableSet()
                inner.forEach { k -> batches[k] = inner }
                BatchRef(inner, mutex)
            }

        /** Removes the element from the global reference AND from whatever [BatchRef] it was associated to. */
        suspend fun remove(element: T) = mutex.withLock { batches.remove(element)?.remove(element) }

        data class BatchRef<T>(private val items: MutableSet<T>, private val mutex: Mutex) {
            suspend fun items() = mutex.withLock(action = items::toSet)
        }
    }

    @OptIn(ExperimentalStdlibApi::class) private fun Any.identityCode() = System.identityHashCode(this).toHexString()
}

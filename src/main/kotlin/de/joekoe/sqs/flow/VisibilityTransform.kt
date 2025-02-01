package de.joekoe.sqs.flow

import de.joekoe.sqs.Message
import de.joekoe.sqs.Queue
import de.joekoe.sqs.SqsConnector
import de.joekoe.sqs.impl.isMessageAlreadyDeleted
import de.joekoe.sqs.utils.identityCode
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory

fun <U : MessageBound, D : MessageBound> Flow<List<U>>.withAutomaticVisibilityExtension(
    connector: SqsConnector,
    extensionDuration: Duration,
    handler: MessageHandler<U, D>,
): Flow<D> = channelFlow {
    val manager = VisibilityManager(connector, extensionDuration, 3.seconds, this)

    this@withAutomaticVisibilityExtension // formatting comment
        .onEach(manager::startTracking)
        .let(handler::handle)
        .onEach(manager::stopTracking)
        .collect(::send)
}

internal class VisibilityManager(
    private val connector: SqsConnector,
    private val extensionDuration: Duration,
    private val extensionThreshold: Duration,
    scope: CoroutineScope,
) : CoroutineScope by scope {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val activeBatches = BatchMap<Message.ReceiptHandle>()

    suspend fun startTracking(messages: List<MessageBound>) {
        messages.groupBy(MessageBound::queue).forEach { (queue, byQueue) ->
            val ref = activeBatches.putBatch(byQueue.map(MessageBound::receiptHandle))
            val interval = queue.visibilityTimeout - extensionThreshold
            schedule(interval, queue, ref)

            logger
                .atDebug()
                .addKeyValue("visibilityBatch.id", ref.identityCode())
                .addKeyValue("visibilityBatch.size", byQueue.size)
                .addKeyValue("queue.name", queue.name)
                .addKeyValue("queue.timeout", queue.visibilityTimeout.toString())
                .addKeyValue("interval", interval.toString())
                .log("Started automatic visibility management")
        }
    }

    suspend fun stopTracking(message: MessageBound) {
        activeBatches.remove(message.receiptHandle)
    }

    private fun schedule(delay: Duration, queue: Queue, reference: Any): Job = launch {
        delay(delay)
        val messages = activeBatches.getBatch(reference)
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
            .onEach {
                if (it.isMessageAlreadyDeleted()) {
                    logger
                        .atDebug()
                        .addKeyValue("message.ref", it.reference)
                        .log("Message was already deleted, ignoring")
                } else {
                    logger
                        .atWarn()
                        .addKeyValue("message.ref", it.reference)
                        .addKeyValue("failure.code", it.code)
                        .addKeyValue("failure.message", it.errorMessage)
                        .addKeyValue("failure.senderFault", it.senderFault)
                        .log("Couldn't extend visibility for message. Will NOT retry")
                }
            }
            .forEach { activeBatches.remove(it.reference) }

        schedule(extensionDuration - extensionThreshold, queue, reference)
    }

    private data class BatchMap<T>(private val batches: MutableMap<T, MutableSet<T>> = mutableMapOf()) {
        private val mutex = Mutex()

        // return value is the actual reference to the batch, this is just a hack to enforce the
        // mutex usage
        suspend fun putBatch(batch: Collection<T>): Any =
            mutex.withLock {
                val inner = batch.toMutableSet()
                inner.forEach { k -> batches[k] = inner }
                inner
            }

        @Suppress("UNCHECKED_CAST") // guarded by put
        suspend fun getBatch(reference: Any): Collection<T> {
            if (reference !is Set<*>) return emptySet()
            return mutex.withLock { reference.toSet() } as Set<T>
        }

        suspend fun remove(element: T) = mutex.withLock { batches.remove(element)?.remove(element) }
    }
}

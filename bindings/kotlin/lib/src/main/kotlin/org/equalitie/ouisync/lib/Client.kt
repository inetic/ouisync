package org.equalitie.ouisync.lib

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.msgpack.core.MessagePack
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.EOFException

/**
 * Receiver for events of type `E`
 */
class EventReceiver<E> internal constructor(
    private val client: Client,
    private val id: Long,
    private val channel: ReceiveChannel<Any?>,
) {
    /**
     * Receives next event
     */
    suspend fun receive(): E {
        @Suppress("UNCHECKED_CAST")
        return channel.receive() as E
    }

    /**
     * Unsubscribes from the events
     */
    suspend fun close() {
        channel.cancel()
        client.unsubscribe(id)
    }

    /**
     * Converts this receiver into [Flow]. The receiver is automatically
     * [close]d after the flow is collected.
     */
    fun consumeAsFlow(): Flow<E> = flow {
        try {
            while (true) {
                emit(receive())
            }
        } finally {
            close()
        }
    }
}

internal class Client {
    var sessionHandle: Long = 0

    private val mutex = Mutex()
    private var nextMessageId: Long = 0
    private val responses: HashMap<Long, CompletableDeferred<Any?>> = HashMap()
    private val subscriptions: HashMap<Long, SendChannel<Any?>> = HashMap()

    suspend fun invoke(request: Request): Any? {
        val id = nextMessageId++

        val stream = ByteArrayOutputStream()
        DataOutputStream(stream).writeLong(id)
        val packer = MessagePack.newDefaultPacker(stream)
        request.pack(packer)
        packer.close()
        val message = stream.toByteArray()

        val deferred = CompletableDeferred<Any?>()

        mutex.withLock {
            responses.put(id, deferred)
        }

        Session.bindings.session_channel_send(sessionHandle, message, message.size)

        return deferred.await()
    }

    suspend fun receive(buffer: ByteArray) {
        val stream = ByteArrayInputStream(buffer)

        val id = try {
            DataInputStream(stream).readLong()
        } catch (e: EOFException) {
            return
        }

        try {
            val unpacker = MessagePack.newDefaultUnpacker(stream)
            val message = ServerMessage.unpack(unpacker)

            when (message) {
                is Success -> handleSuccess(id, message.value)
                is Failure -> handleFailure(id, message.error)
                is Notification -> handleNotification(id, message.content)
            }
        } catch (e: InvalidResponse) {
            handleFailure(id, e)
        } catch (e: InvalidNotification) {
            // TODO: log?
        } catch (e: Exception) {
            // TODO: log?
        }
    }

    suspend fun <E> subscribe(request: Request): EventReceiver<E> {
        // FIXME: race condition - if a notification arrives after we call `invoke` but before we
        // register the sender then it gets silently dropped

        val id = invoke(request) as Long
        val channel = Channel<Any?>(1024)

        mutex.withLock {
            subscriptions.put(id, channel)
        }

        return EventReceiver(this, id, channel)
    }

    suspend fun unsubscribe(id: Long) {
        mutex.withLock {
            subscriptions.remove(id)
        }

        invoke(Unsubscribe(id))
    }

    private suspend fun handleSuccess(id: Long, content: Any?) {
        mutex.withLock {
            responses.remove(id)?.complete(content)
        }
    }

    private suspend fun handleNotification(id: Long, content: Any?) {
        mutex.withLock {
            subscriptions.get(id)?.send(content)
        }
    }

    private suspend fun handleFailure(id: Long, error: Exception) {
        mutex.withLock {
            responses.remove(id)?.completeExceptionally(error)
        }
    }
}

private data class ServerEnvelope(val id: Long, val content: ServerMessage)

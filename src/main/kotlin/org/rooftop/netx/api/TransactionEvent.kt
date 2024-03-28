package org.rooftop.netx.api

import kotlin.reflect.KClass

sealed class TransactionEvent(
    val transactionId: String,
    val nodeName: String,
    val group: String,
    internal val event: String?,
    internal val codec: Codec,
    internal var nextEvent: Any? = null,
) {

    fun <T : Any> setNextEvent(event: T): T {
        this.nextEvent = event
        return event
    }

    fun <T : Any> decodeEvent(type: Class<T>): T = decodeEvent(type.kotlin)

    fun <T : Any> decodeEvent(type: KClass<T>): T =
        codec.decode(
            event ?: throw NullPointerException("Cannot decode event cause event is null"),
            type
        )

    internal abstract fun copy(): TransactionEvent
}

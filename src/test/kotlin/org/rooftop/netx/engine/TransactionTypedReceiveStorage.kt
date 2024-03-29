package org.rooftop.netx.engine

import io.kotest.matchers.equals.shouldBeEqual
import org.rooftop.netx.api.TransactionEvent
import org.rooftop.netx.api.TransactionStartEvent
import org.rooftop.netx.api.TransactionStartListener
import org.rooftop.netx.meta.TransactionHandler
import reactor.core.publisher.Mono
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.reflect.KClass

@TransactionHandler
class TransactionTypedReceiveStorage {

    private val storage: ConcurrentMap<KClass<*>, CopyOnWriteArrayList<TransactionEvent>> =
        ConcurrentHashMap()

    fun clear() {
        storage.clear()
    }

    fun handlerShouldBeEqual(key: KClass<*>, count: Int) {
        (storage[key]?.size ?: 0) shouldBeEqual count
    }

    @TransactionStartListener
    fun any(transaction: TransactionStartEvent): Mono<Unit> {
        return Mono.fromCallable { log(Any::class, transaction) }
    }

    @TransactionStartListener(NetxEventTypedDispatherTest.Foo::class)
    fun foo(transaction: TransactionStartEvent): Mono<Unit> {
        return Mono.fromCallable { log(NetxEventTypedDispatherTest.Foo::class, transaction) }
    }

    @TransactionStartListener(String::class)
    fun string(transaction: TransactionStartEvent) {
        log(String::class, transaction)
    }

    @TransactionStartListener(Long::class)
    fun long(transaction: TransactionStartEvent): Long {
        log(Long::class, transaction)
        return 1L
    }

    @TransactionStartListener(Unit::class)
    fun unit(transaction: TransactionStartEvent) {
        log(Unit::class, transaction)
    }

    @TransactionStartListener(Boolean::class)
    fun boolean(transaction: TransactionStartEvent) {
        log(Boolean::class, transaction)
    }

    private fun log(key: KClass<*>, transaction: TransactionEvent) {
        storage.putIfAbsent(key, CopyOnWriteArrayList())
        storage[key]?.add(transaction)
    }

}

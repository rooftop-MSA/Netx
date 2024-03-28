package org.rooftop.netx.client

import io.kotest.matchers.ints.shouldBeGreaterThanOrEqual
import org.rooftop.netx.api.*
import org.rooftop.netx.meta.TransactionHandler
import reactor.core.publisher.Mono
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.CopyOnWriteArrayList

@TransactionHandler
class TransactionReceiveStorage(
    private val storage: ConcurrentMap<String, CopyOnWriteArrayList<TransactionEvent>> = ConcurrentHashMap(),
) {

    fun clear() {
        storage.clear()
    }

    fun joinCountShouldBeGreaterThanOrEqual(count: Int) {
        (storage["JOIN"]?.size ?: 0) shouldBeGreaterThanOrEqual count
    }

    fun startCountShouldBeGreaterThanOrEqual(count: Int) {
        (storage["START"]?.size ?: 0) shouldBeGreaterThanOrEqual count
    }

    fun commitCountShouldBeGreaterThanOrEqual(count: Int) {
        (storage["COMMIT"]?.size ?: 0) shouldBeGreaterThanOrEqual count
    }

    fun rollbackCountShouldBeGreaterThanOrEqual(count: Int) {
        (storage["ROLLBACK"]?.size ?: 0) shouldBeGreaterThanOrEqual count
    }

    @TransactionStartListener(
        event = NetxLoadTest.LoadTestEvent::class,
        successWith = SuccessWith.PUBLISH_JOIN
    )
    fun listenStart(transaction: TransactionStartEvent): Mono<Unit> {
        return Mono.fromCallable { saveTransaction("START", transaction) }
            .map { transaction.decodeEvent(NetxLoadTest.LoadTestEvent::class) }
            .map { transaction.setNextEvent(it) }
    }

    @TransactionJoinListener(
        event = NetxLoadTest.LoadTestEvent::class,
        successWith = SuccessWith.PUBLISH_COMMIT
    )
    fun listenJoin(transaction: TransactionJoinEvent): Mono<Unit> {
        return Mono.fromCallable { saveTransaction("JOIN", transaction) }
            .map { transaction.decodeEvent(NetxLoadTest.LoadTestEvent::class) }
            .map { transaction.setNextEvent(it) }
    }

    @TransactionRollbackListener(event = NetxLoadTest.LoadTestEvent::class)
    fun listenRollback(transaction: TransactionRollbackEvent): Mono<Unit> {
        return Mono.fromCallable { saveTransaction("ROLLBACK", transaction) }
    }

    @TransactionCommitListener(event = NetxLoadTest.LoadTestEvent::class)
    fun listenCommit(transaction: TransactionCommitEvent): Mono<Unit> {
        return Mono.fromCallable { saveTransaction("COMMIT", transaction) }
            .map { transaction.decodeEvent(NetxLoadTest.LoadTestEvent::class) }
            .map {
                transaction.setNextEvent(it)
                if (it.load == "-") {
                    throw IllegalArgumentException("Rollback cause \"-\"")
                }
            }
    }

    private fun saveTransaction(key: String, transaction: TransactionEvent) {
        storage.putIfAbsent(key, CopyOnWriteArrayList())
        storage[key]?.add(transaction)
    }
}

package org.rooftop.netx.engine

import org.rooftop.netx.api.*
import org.rooftop.netx.meta.TransactionHandler
import java.util.*

@TransactionHandler
class TransactionReceiveStorage {

    private val startEvents: Queue<TransactionStartEvent> = LinkedList()
    private val joinEvents: Queue<TransactionJoinEvent> = LinkedList()
    private val rollbackEvents: Queue<TransactionRollbackEvent> = LinkedList()
    private val commitEvents: Queue<TransactionCommitEvent> = LinkedList()

    fun clear(){
        startEvents.clear()
        joinEvents.clear()
        rollbackEvents.clear()
        commitEvents.clear()
    }

    @TransactionStartListener(successWith = SuccessWith.END)
    fun handleStart(transactionStartEvent: TransactionStartEvent) {
        startEvents.add(transactionStartEvent)
    }

    @TransactionJoinListener(successWith = SuccessWith.END)
    fun handleJoin(transactionJoinEvent: TransactionJoinEvent) {
        joinEvents.add(transactionJoinEvent)
    }

    @TransactionRollbackListener
    fun handleRollback(transactionRollbackEvent: TransactionRollbackEvent) {
        rollbackEvents.add(transactionRollbackEvent)
    }

    @TransactionCommitListener
    fun handleCommit(transactionCommitEvent: TransactionCommitEvent) {
        commitEvents.add(transactionCommitEvent)
    }

    fun pollStart(): TransactionStartEvent = startEvents.poll()

    fun pollJoin(): TransactionJoinEvent = joinEvents.poll()

    fun pollRollback(): TransactionRollbackEvent = rollbackEvents.poll()

    fun pollCommit(): TransactionCommitEvent = commitEvents.poll()

}

package org.rooftop.netx.redis

import org.rooftop.netx.api.*
import org.rooftop.netx.meta.TransactionHandler

@TransactionHandler
class NoPublisherTransactionHandlerAssertions : AbstractTransactionHandlerAssertions() {

    @TransactionRollbackListener
    fun handleRollback(event: TransactionRollbackEvent): Long {
        put("ROLLBACK")
        return Long.MIN_VALUE
    }

    @TransactionCommitListener
    fun handleCommit(event: TransactionCommitEvent) {
        put("COMMIT")
    }

    @TransactionStartListener(successWith = SuccessWith.END)
    fun handleStart(event: TransactionStartEvent): Foo {
        put("START")
        return Foo("START")
    }

    @TransactionJoinListener(successWith = SuccessWith.END)
    fun handleJoin(event: TransactionJoinEvent): Any {
        put("JOIN")
        return Any::class
    }

    class Foo(name: String)

}

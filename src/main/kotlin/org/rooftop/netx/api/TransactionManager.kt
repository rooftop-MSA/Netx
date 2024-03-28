package org.rooftop.netx.api

import reactor.core.publisher.Mono

interface TransactionManager {

    fun start(): Mono<String>

    fun <T : Any> start(event: T): Mono<String>

    fun syncStart(): String

    fun <T : Any> syncStart(event: T): String

    fun join(transactionId: String): Mono<String>

    fun <T : Any> join(transactionId: String, event: T): Mono<String>

    fun syncJoin(transactionId: String): String

    fun <T : Any> syncJoin(transactionId: String, event: T): String

    fun exists(transactionId: String): Mono<String>

    fun syncExists(transactionId: String): String

    fun commit(transactionId: String): Mono<String>

    fun <T : Any> commit(transactionId: String, event: T): Mono<String>

    fun syncCommit(transactionId: String): String

    fun <T : Any> syncCommit(transactionId: String, event: T): String

    fun rollback(transactionId: String, cause: String): Mono<String>

    fun <T : Any> rollback(transactionId: String, cause: String, event: T): Mono<String>

    fun syncRollback(transactionId: String, cause: String): String

    fun <T : Any> syncRollback(transactionId: String, cause: String, event: T): String

}

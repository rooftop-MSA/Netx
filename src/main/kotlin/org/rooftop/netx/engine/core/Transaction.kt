package org.rooftop.netx.engine.core

internal data class Transaction(
    val id: String,
    val serverId: String,
    val group: String,
    val state: TransactionState,
    val cause: String? = null,
    val event: String? = null,
)

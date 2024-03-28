package org.rooftop.netx.api

class TransactionRollbackEvent internal constructor(
    transactionId: String,
    nodeName: String,
    group: String,
    event: String?,
    val cause: String,
    codec: Codec,
) : TransactionEvent(transactionId, nodeName, group, event, codec) {

    override fun copy(): TransactionEvent =
        TransactionJoinEvent(transactionId, nodeName, group, event, codec)
}

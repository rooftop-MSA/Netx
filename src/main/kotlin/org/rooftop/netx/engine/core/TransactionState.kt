package org.rooftop.netx.engine.core

internal enum class TransactionState {
    JOIN,
    COMMIT,
    ROLLBACK,
    START,
    ;
}

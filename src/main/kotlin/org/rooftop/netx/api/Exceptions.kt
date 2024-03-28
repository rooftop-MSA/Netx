package org.rooftop.netx.api

class EncodeException(message: String, throwable: Throwable) : RuntimeException(message, throwable)

class DecodeException(message: String, throwable: Throwable) : RuntimeException(message, throwable)

open class TransactionException(message: String) : RuntimeException(message)

class AlreadyCommittedTransactionException(transactionId: String, state: String) :
    TransactionException("Cannot join transaction cause, transaction \"$transactionId\" already \"$state\"")

class NotFoundDispatchFunctionException(message: String) : RuntimeException(message)

class FailedAckTransactionException(message: String) : RuntimeException(message)

class ResultTimeoutException(message: String, throwable: Throwable) :
    RuntimeException(message, throwable)

class ResultException(message: String) : RuntimeException(message)

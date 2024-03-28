package org.rooftop.netx.api

import kotlin.reflect.KClass

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class TransactionCommitListener(
    val event: KClass<*> = Any::class,
    val noRollbackFor: Array<KClass<out Throwable>> = [],
)

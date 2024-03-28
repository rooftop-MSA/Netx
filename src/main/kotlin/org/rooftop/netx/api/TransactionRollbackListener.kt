package org.rooftop.netx.api

import kotlin.reflect.KClass

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class TransactionRollbackListener(
    val event: KClass<*> = Any::class,
)

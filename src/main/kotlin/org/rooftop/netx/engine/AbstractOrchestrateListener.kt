package org.rooftop.netx.engine

import org.rooftop.netx.api.*
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import kotlin.reflect.KClass

internal abstract class AbstractOrchestrateListener<T : Any, V : Any> internal constructor(
    private val orchestratorId: String,
    internal val orchestrateSequence: Int,
    private val codec: Codec,
    private val transactionManager: TransactionManager,
    private val requestHolder: RequestHolder,
    private val resultHolder: ResultHolder,
    private val typeReference: TypeReference<T>?,
) {

    var isFirst: Boolean = true
    var isLast: Boolean = true
    var isRollbackable: Boolean = false
    var beforeRollbackOrchestrateSequence: Int = 0
    var rollbackSequence: Int = orchestrateSequence

    private var nextOrchestrateListener: AbstractOrchestrateListener<V, Any>? = null
    private var nextRollbackOrchestrateListener: AbstractOrchestrateListener<V, Any>? = null
    private var castableType: KClass<out T>? = null

    @Suppress("UNCHECKED_CAST")
    internal fun setNextOrchestrateListener(nextOrchestrateListener: AbstractOrchestrateListener<out Any, out Any>) {
        this.nextOrchestrateListener =
            nextOrchestrateListener as AbstractOrchestrateListener<V, Any>
    }

    @Suppress("UNCHECKED_CAST")
    internal fun setNextRollbackOrchestrateListener(nextRollbackOrchestrateListener: AbstractOrchestrateListener<out Any, out Any>) {
        this.nextRollbackOrchestrateListener =
            nextRollbackOrchestrateListener as AbstractOrchestrateListener<V, Any>
    }

    internal fun setCastableType(type: KClass<out T>) {
        castableType = type
    }

    internal fun Mono<Pair<V, Context>>.setNextCastableType(): Mono<Pair<V, Context>> {
        return this.doOnNext { (request, _) ->
            nextOrchestrateListener?.castableType = request::class
            nextRollbackOrchestrateListener?.castableType = request::class
        }
    }

    protected fun Mono<OrchestrateEvent>.mapReifiedRequest(): Mono<Pair<T, OrchestrateEvent>> {
        return this.map { event ->
            if (typeReference == null) {
                return@map codec.decode(event.clientEvent, getCastableType()) to event
            }
            codec.decode(event.clientEvent, typeReference) to event
        }
    }

    protected fun Mono<OrchestrateEvent>.getHeldRequest(transactionEvent: TransactionEvent): Mono<Pair<T, OrchestrateEvent>> {
        return this.flatMap { event ->
            val key = "${transactionEvent.transactionId}:$orchestrateSequence"
            if (typeReference == null) {
                return@flatMap requestHolder.getRequest(key, getCastableType())
                    .map { it to event }
            }
            requestHolder.getRequest(key, typeReference).map { it to event }
        }
    }

    protected fun holdRequestIfRollbackable(request: T, transactionId: String): Mono<T> {
        if (!isRollbackable) {
            Mono.just(request)
        }
        return requestHolder.setRequest(
            "$transactionId:$orchestrateSequence",
            request
        )
    }

    protected fun Mono<Pair<V, Context>>.toOrchestrateEvent(): Mono<OrchestrateEvent> {
        return this.map { (response, context) ->
            OrchestrateEvent(
                orchestratorId = orchestratorId,
                orchestrateSequence = orchestrateSequence + 1,
                clientEvent = codec.encode(response),
                context = codec.encode(context.contexts),
            )
        }
    }

    private fun getCastableType(): KClass<out T> {
        return castableType
            ?: throw NullPointerException("OrchestratorId \"$orchestratorId\", OrchestrateSequence \"$orchestrateSequence\"'s CastableType was null")
    }

    protected fun cast(data: String): T {
        return castableType?.let {
            codec.decode(data, it)
        } ?: throw NullPointerException("Cannot cast \"$data\" cause, castableType is null")
    }

    protected fun TransactionEvent.startWithOrchestrateEvent(): Mono<OrchestrateEvent> =
        Mono.just(this.decodeEvent(OrchestrateEvent::class))

    private fun Throwable.toEmptyStackTrace(): Throwable {
        this.stackTrace = arrayOf()
        return this
    }

    protected fun rollback(
        transactionId: String,
        throwable: Throwable,
        orchestrateEvent: OrchestrateEvent,
    ) {
        val rollbackOrchestrateEvent =
            OrchestrateEvent(
                orchestrateEvent.orchestratorId,
                rollbackSequence,
                "",
                orchestrateEvent.context,
            )
        holdFailResult(transactionId, throwable)
            .flatMap {
                transactionManager.rollback(
                    transactionId = transactionId,
                    cause = throwable.message ?: throwable.localizedMessage,
                    event = rollbackOrchestrateEvent
                )
            }.subscribeOn(Schedulers.parallel()).subscribe()
    }

    private fun holdFailResult(transactionId: String, throwable: Throwable): Mono<Throwable> {
        return resultHolder.setFailResult(transactionId, throwable.toEmptyStackTrace())
    }

    open fun withAnnotated(): AbstractOrchestrateListener<T, V> {
        return this
    }

    override fun toString(): String {
        return "${this.javaClass.name}(orchestrateSequence=$orchestrateSequence, " +
                "isFirst=$isFirst, isLast=$isLast, isRollbackable=$isRollbackable, " +
                "beforeRollbackOrchestrateSequence=$beforeRollbackOrchestrateSequence, " +
                "rollbackSequence=$rollbackSequence)"
    }
}

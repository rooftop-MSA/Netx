package org.rooftop.netx.engine.listen

import org.rooftop.netx.api.*
import org.rooftop.netx.engine.AbstractOrchestrateListener
import org.rooftop.netx.engine.OrchestrateEvent
import org.rooftop.netx.engine.RequestHolder
import org.rooftop.netx.engine.ResultHolder
import reactor.core.publisher.Mono

internal class RollbackOrchestrateListener<T : Any, V : Any>(
    private val codec: Codec,
    private val orchestratorId: String,
    orchestrateSequence: Int,
    private val transactionManager: TransactionManager,
    private val rollbackCommand: RollbackCommand<T>,
    requestHolder: RequestHolder,
    resultHolder: ResultHolder,
    typeReference: TypeReference<T>?,
) : AbstractOrchestrateListener<T, V>(
    orchestratorId,
    orchestrateSequence,
    codec,
    transactionManager,
    requestHolder,
    resultHolder,
    typeReference,
) {

    @TransactionRollbackListener(OrchestrateEvent::class)
    fun listenRollbackOrchestrateEvent(transactionRollbackEvent: TransactionRollbackEvent): Mono<Unit> {
        return transactionRollbackEvent.toOrchestrateEvent()
            .filter { it.orchestratorId == orchestratorId && it.orchestrateSequence == orchestrateSequence }
            .getHeldRequest(transactionRollbackEvent)
            .map { (request, event) ->
                rollbackCommand.command(request, event.context)
            }
            .map {
                if (it.first == null) {
                    return@map "ROLLBACK SUCCESS" to it.second
                }
                it
            }
            .cascadeRollback(transactionRollbackEvent)
    }

    private fun Mono<Pair<Any?, Context>>.cascadeRollback(transactionRollbackEvent: TransactionRollbackEvent): Mono<Unit> {
        return this.filter { !isFirst }
            .flatMap { (_, context) ->
                val orchestrateEvent = transactionRollbackEvent.decodeEvent(OrchestrateEvent::class)
                val nextOrchestrateEvent = OrchestrateEvent(
                    orchestrateEvent.orchestratorId,
                    beforeRollbackOrchestrateSequence,
                    orchestrateEvent.clientEvent,
                    codec.encode(context.contexts),
                )
                transactionManager.rollback(
                    transactionRollbackEvent.transactionId,
                    transactionRollbackEvent.cause,
                    nextOrchestrateEvent
                )
            }.map { }
    }
}

package org.rooftop.netx.engine.listen

import org.rooftop.netx.api.*
import org.rooftop.netx.engine.AbstractOrchestrateListener
import org.rooftop.netx.engine.OrchestrateEvent
import org.rooftop.netx.engine.RequestHolder
import org.rooftop.netx.engine.ResultHolder
import reactor.core.publisher.Mono

internal class MonoJoinOrchestrateListener<T : Any, V : Any>(
    codec: Codec,
    private val transactionManager: TransactionManager,
    private val orchestratorId: String,
    orchestrateSequence: Int,
    private val monoOrchestrateCommand: MonoOrchestrateCommand<T, V>,
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

    @TransactionJoinListener(OrchestrateEvent::class)
    fun listenJoinOrchestrateEvent(transactionJoinEvent: TransactionJoinEvent): Mono<Unit> {
        return transactionJoinEvent.toOrchestrateEvent()
            .filter {
                it.orchestrateSequence == orchestrateSequence && it.orchestratorId == orchestratorId
            }
            .mapReifiedRequest()
            .flatMap { (request, event) ->
                holdRequestIfRollbackable(request, transactionJoinEvent.transactionId)
                    .map { it to event }
            }
            .flatMap { (request, event) ->
                monoOrchestrateCommand.command(request, event.context)
            }
            .setNextCastableType()
            .onErrorRollback(
                transactionJoinEvent.transactionId,
                transactionJoinEvent.decodeEvent(OrchestrateEvent::class)
            )
            .toOrchestrateEvent()
            .flatMap {
                if (isLast) {
                    return@flatMap transactionManager.commit(
                        transactionId = transactionJoinEvent.transactionId,
                        event = it,
                    )
                }
                transactionManager.join(
                    transactionId = transactionJoinEvent.transactionId,
                    undo = "",
                    event = it,
                )
            }
            .onErrorResume {
                if (it::class == AlreadyCommittedTransactionException::class) {
                    return@onErrorResume Mono.empty()
                }
                throw it
            }
            .map { }
    }
}

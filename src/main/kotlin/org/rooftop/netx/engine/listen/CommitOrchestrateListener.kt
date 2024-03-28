package org.rooftop.netx.engine.listen

import org.rooftop.netx.api.*
import org.rooftop.netx.engine.AbstractOrchestrateListener
import org.rooftop.netx.engine.OrchestrateEvent
import org.rooftop.netx.engine.RequestHolder
import org.rooftop.netx.engine.ResultHolder
import reactor.core.publisher.Mono

internal class CommitOrchestrateListener<T : Any, V : Any> internal constructor(
    codec: Codec,
    transactionManager: TransactionManager,
    private val orchestratorId: String,
    orchestrateSequence: Int,
    private val orchestrateCommand: OrchestrateCommand<T, V>,
    private val resultHolder: ResultHolder,
    requestHolder: RequestHolder,
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

    @TransactionCommitListener(OrchestrateEvent::class)
    fun listenCommitOrchestrateEvent(transactionCommitEvent: TransactionCommitEvent): Mono<V> {
        return Mono.just(transactionCommitEvent)
            .map { it.decodeEvent(OrchestrateEvent::class) }
            .filter { it.orchestrateSequence == orchestrateSequence && it.orchestratorId == orchestratorId }
            .mapReifiedRequest()
            .flatMap { (request, event) ->
                holdRequestIfRollbackable(request, transactionCommitEvent.transactionId)
                    .map{ it to event }
            }
            .map { (request, event) ->
                orchestrateCommand.command(request, event.context)
            }
            .doOnError {
                rollback(
                    transactionCommitEvent.transactionId,
                    it,
                    transactionCommitEvent.decodeEvent(OrchestrateEvent::class)
                )
            }
            .flatMap { (response, _) ->
                resultHolder.setSuccessResult(transactionCommitEvent.transactionId, response)
            }
    }
}

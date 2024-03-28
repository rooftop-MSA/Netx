package org.rooftop.netx.engine

import org.rooftop.netx.api.*
import org.rooftop.netx.engine.listen.*
import reactor.core.publisher.Mono

class DefaultOrchestrateChain<OriginReq : Any, T : Any, V : Any> private constructor(
    private val orchestratorId: String,
    private val orchestrateSequence: Int,
    private val chainContainer: ChainContainer,
    private var orchestrateListener: AbstractOrchestrateListener<T, V>,
    private var rollbackOrchestrateListener: AbstractOrchestrateListener<T, *>?,
    private val beforeDefaultOrchestrateChain: DefaultOrchestrateChain<OriginReq, out Any, T>? = null,
) : OrchestrateChain<OriginReq, T, V> {

    private var nextDefaultOrchestrateChain: DefaultOrchestrateChain<OriginReq, V, out Any>? = null

    override fun <S : Any> join(
        orchestrate: Orchestrate<V, S>,
        rollback: Rollback<V, *>?,
    ): DefaultOrchestrateChain<OriginReq, V, S> {
        val nextJoinOrchestrateListener =
            getJoinOrchestrateListener<V, S>(CommandType.DEFAULT, orchestrate)
        val nextRollbackOrchestrateListener =
            getRollbackOrchestrateListener<V, S>(CommandType.DEFAULT, rollback)

        return nextOrchestrateChain(nextJoinOrchestrateListener, nextRollbackOrchestrateListener)
    }

    override fun <S : Any> joinWithContext(
        contextOrchestrate: ContextOrchestrate<V, S>,
        contextRollback: ContextRollback<V, *>?
    ): DefaultOrchestrateChain<OriginReq, V, S> {
        val nextJoinOrchestrateListener =
            getJoinOrchestrateListener<V, S>(CommandType.CONTEXT, contextOrchestrate)
        val nextRollbackOrchestrateListener =
            getRollbackOrchestrateListener<V, S>(CommandType.CONTEXT, contextRollback)

        return nextOrchestrateChain(nextJoinOrchestrateListener, nextRollbackOrchestrateListener)
    }

    private fun <T : Any, V : Any> getJoinOrchestrateListener(
        commandType: CommandType,
        function: TypeReified<T>,
    ) = JoinOrchestrateListener(
        codec = chainContainer.codec,
        transactionManager = chainContainer.transactionManager,
        orchestratorId = orchestratorId,
        orchestrateSequence = orchestrateSequence + 1,
        orchestrateCommand = OrchestrateCommand<T, V>(
            commandType,
            chainContainer.codec,
            function
        ),
        requestHolder = chainContainer.requestHolder,
        resultHolder = chainContainer.resultHolder,
        typeReference = function.reified(),
    )

    private fun <S : Any> nextOrchestrateChain(
        nextJoinOrchestrateListener: JoinOrchestrateListener<V, S>,
        nextRollbackOrchestrateListener: RollbackOrchestrateListener<V, S>?
    ): DefaultOrchestrateChain<OriginReq, V, S> {
        val nextDefaultOrchestrateChain = DefaultOrchestrateChain(
            orchestratorId,
            orchestrateSequence + 1,
            chainContainer,
            nextJoinOrchestrateListener,
            nextRollbackOrchestrateListener,
            this,
        )
        this.nextDefaultOrchestrateChain = nextDefaultOrchestrateChain

        return nextDefaultOrchestrateChain
    }

    override fun <S : Any> joinReactive(
        orchestrate: Orchestrate<V, Mono<S>>,
        rollback: Rollback<V, Mono<*>>?,
    ): DefaultOrchestrateChain<OriginReq, V, S> {
        val nextJoinOrchestrateListener =
            getMonoJoinOrchestrateListener<V, S>(CommandType.DEFAULT, orchestrate)
        val nextRollbackOrchestrateListener =
            getMonoRollbackOrchestrateListener<V, S>(CommandType.DEFAULT, rollback)

        return nextOrchestrateChain(nextJoinOrchestrateListener, nextRollbackOrchestrateListener)
    }

    override fun <S : Any> joinReactiveWithContext(
        contextOrchestrate: ContextOrchestrate<V, Mono<S>>,
        contextRollback: ContextRollback<V, Mono<*>>?
    ): DefaultOrchestrateChain<OriginReq, V, S> {
        val nextJoinOrchestrateListener =
            getMonoJoinOrchestrateListener<V, S>(CommandType.CONTEXT, contextOrchestrate)
        val nextRollbackOrchestrateListener =
            getMonoRollbackOrchestrateListener<V, S>(CommandType.CONTEXT, contextRollback)

        val nextDefaultOrchestrateChain = DefaultOrchestrateChain(
            orchestratorId,
            orchestrateSequence + 1,
            chainContainer,
            nextJoinOrchestrateListener,
            nextRollbackOrchestrateListener,
            this,
        )
        this.nextDefaultOrchestrateChain = nextDefaultOrchestrateChain

        return nextDefaultOrchestrateChain
    }

    private fun <T : Any, V : Any> getMonoJoinOrchestrateListener(
        commandType: CommandType,
        function: TypeReified<T>,
    ) = MonoJoinOrchestrateListener(
        codec = chainContainer.codec,
        transactionManager = chainContainer.transactionManager,
        orchestratorId = orchestratorId,
        orchestrateSequence = orchestrateSequence + 1,
        monoOrchestrateCommand = MonoOrchestrateCommand<T, V>(
            commandType,
            chainContainer.codec,
            function
        ),
        requestHolder = chainContainer.requestHolder,
        resultHolder = chainContainer.resultHolder,
        function.reified(),
    )

    private fun <S : Any> nextOrchestrateChain(
        nextJoinOrchestrateListener: MonoJoinOrchestrateListener<V, S>,
        nextRollbackOrchestrateListener: MonoRollbackOrchestrateListener<V, S>?
    ): DefaultOrchestrateChain<OriginReq, V, S> {
        val nextDefaultOrchestrateChain = DefaultOrchestrateChain(
            orchestratorId,
            orchestrateSequence + 1,
            chainContainer,
            nextJoinOrchestrateListener,
            nextRollbackOrchestrateListener,
            this,
        )
        this.nextDefaultOrchestrateChain = nextDefaultOrchestrateChain

        return nextDefaultOrchestrateChain
    }

    override fun <S : Any> commit(
        orchestrate: Orchestrate<V, S>,
        rollback: Rollback<V, *>?,
    ): Orchestrator<OriginReq, S> {
        val nextCommitOrchestrateListener =
            getCommitOrchestrateListener<V, S>(CommandType.DEFAULT, orchestrate)
        val nextRollbackOrchestrateListener =
            getRollbackOrchestrateListener<V, S>(CommandType.DEFAULT, rollback)

        return createOrchestrator(nextCommitOrchestrateListener, nextRollbackOrchestrateListener)
    }

    override fun <S : Any> commitWithContext(
        contextOrchestrate: ContextOrchestrate<V, S>,
        contextRollback: ContextRollback<V, *>?
    ): Orchestrator<OriginReq, S> {
        val nextCommitOrchestrateListener =
            getCommitOrchestrateListener<V, S>(CommandType.CONTEXT, contextOrchestrate)
        val nextRollbackOrchestrateListener =
            getRollbackOrchestrateListener<V, S>(CommandType.CONTEXT, contextRollback)

        return createOrchestrator(nextCommitOrchestrateListener, nextRollbackOrchestrateListener)
    }

    private fun <T : Any, V : Any> getCommitOrchestrateListener(
        commandType: CommandType,
        function: TypeReified<T>,
    ) = CommitOrchestrateListener(
        codec = chainContainer.codec,
        transactionManager = chainContainer.transactionManager,
        orchestratorId = orchestratorId,
        orchestrateSequence = orchestrateSequence + 1,
        orchestrateCommand = OrchestrateCommand<T, V>(commandType, chainContainer.codec, function),
        resultHolder = chainContainer.resultHolder,
        requestHolder = chainContainer.requestHolder,
        function.reified(),
    )

    private fun <T : Any, V : Any> getRollbackOrchestrateListener(
        commandType: CommandType,
        rollback: TypeReified<T>?
    ) = rollback?.let {
        RollbackOrchestrateListener<T, V>(
            codec = chainContainer.codec,
            transactionManager = chainContainer.transactionManager,
            orchestratorId = orchestratorId,
            orchestrateSequence = orchestrateSequence + 1,
            rollbackCommand = RollbackCommand(commandType, chainContainer.codec, it),
            requestHolder = chainContainer.requestHolder,
            resultHolder = chainContainer.resultHolder,
            typeReference = it.reified(),
        )
    }

    private fun <S : Any> createOrchestrator(
        nextCommitOrchestrateListener: CommitOrchestrateListener<V, S>,
        nextRollbackOrchestrateListener: RollbackOrchestrateListener<V, S>?
    ): Orchestrator<OriginReq, S> {
        return chainContainer.orchestratorCache.cache(orchestratorId) {
            val nextDefaultOrchestrateChain = DefaultOrchestrateChain(
                orchestratorId,
                orchestrateSequence + 1,
                chainContainer,
                nextCommitOrchestrateListener,
                nextRollbackOrchestrateListener,
                this,
            )
            this.nextDefaultOrchestrateChain = nextDefaultOrchestrateChain
            val firstOrchestrators = nextDefaultOrchestrateChain.initOrchestrateListeners()

            return@cache OrchestratorManager<OriginReq, S>(
                transactionManager = chainContainer.transactionManager,
                codec = chainContainer.codec,
                orchestratorId = orchestratorId,
                resultHolder = chainContainer.resultHolder,
                orchestrateListener = firstOrchestrators.first,
                rollbackOrchestrateListener = firstOrchestrators.second,
            )
        }
    }

    override fun <S : Any> commitReactive(
        orchestrate: Orchestrate<V, Mono<S>>,
        rollback: Rollback<V, Mono<*>>?,
    ): Orchestrator<OriginReq, S> {
        val nextJoinOrchestrateListener =
            getMonoCommitOrchestrateListener<V, S>(CommandType.DEFAULT, orchestrate)
        val nextRollbackOrchestrateListener =
            getMonoRollbackOrchestrateListener<V, S>(CommandType.DEFAULT, rollback)

        return createOrchestrator(nextJoinOrchestrateListener, nextRollbackOrchestrateListener)
    }

    override fun <S : Any> commitReactiveWithContext(
        contextOrchestrate: ContextOrchestrate<V, Mono<S>>,
        contextRollback: ContextRollback<V, Mono<*>>?
    ): Orchestrator<OriginReq, S> {
        val nextJoinOrchestrateListener =
            getMonoCommitOrchestrateListener<V, S>(CommandType.CONTEXT, contextOrchestrate)
        val nextRollbackOrchestrateListener =
            getMonoRollbackOrchestrateListener<V, S>(CommandType.CONTEXT, contextRollback)

        return createOrchestrator(nextJoinOrchestrateListener, nextRollbackOrchestrateListener)
    }

    private fun <S : Any> createOrchestrator(
        nextJoinOrchestrateListener: MonoCommitOrchestrateListener<V, S>,
        nextRollbackOrchestrateListener: MonoRollbackOrchestrateListener<V, S>?
    ): Orchestrator<OriginReq, S> {
        return chainContainer.orchestratorCache.cache(orchestratorId) {
            val nextDefaultOrchestrateChain = DefaultOrchestrateChain(
                orchestratorId,
                orchestrateSequence + 1,
                chainContainer,
                nextJoinOrchestrateListener,
                nextRollbackOrchestrateListener,
                this,
            )
            this.nextDefaultOrchestrateChain = nextDefaultOrchestrateChain

            val firstOrchestrators = nextDefaultOrchestrateChain.initOrchestrateListeners()

            return@cache OrchestratorManager<OriginReq, S>(
                transactionManager = chainContainer.transactionManager,
                codec = chainContainer.codec,
                orchestratorId = orchestratorId,
                resultHolder = chainContainer.resultHolder,
                orchestrateListener = firstOrchestrators.first,
                rollbackOrchestrateListener = firstOrchestrators.second
            )
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun initOrchestrateListeners(): Pair<AbstractOrchestrateListener<OriginReq, Any>, AbstractOrchestrateListener<OriginReq, Any>?> {
        val annotatedListeners = getAllOrchestrateListeners()
            .toAnnotatedListeners()

        chainOrchestrateListeners(annotatedListeners)
        chainRollbackListeners(annotatedListeners)

        addDispatcher(annotatedListeners)

        return annotatedListeners[0] as Pair<AbstractOrchestrateListener<OriginReq, Any>, AbstractOrchestrateListener<OriginReq, Any>?>
    }

    private fun getAllOrchestrateListeners(): MutableList<Pair<AbstractOrchestrateListener<out Any, out Any>, AbstractOrchestrateListener<out Any, out Any>?>> {
        val orchestrateListeners = mutableListOf<
                Pair<AbstractOrchestrateListener<out Any, out Any>, AbstractOrchestrateListener<out Any, out Any>?>>()

        var defaultOrchestrateChainCursor: DefaultOrchestrateChain<OriginReq, out Any, out Any>? =
            this
        while (defaultOrchestrateChainCursor != null) {
            orchestrateListeners.add(
                defaultOrchestrateChainCursor.orchestrateListener
                        to defaultOrchestrateChainCursor.rollbackOrchestrateListener
            )
            if (defaultOrchestrateChainCursor.beforeDefaultOrchestrateChain == null) {
                break
            }
            defaultOrchestrateChainCursor =
                defaultOrchestrateChainCursor.beforeDefaultOrchestrateChain
        }

        orchestrateListeners.reverse()

        return orchestrateListeners
    }

    private fun List<Pair<AbstractOrchestrateListener<out Any, out Any>, AbstractOrchestrateListener<out Any, out Any>?>>.toAnnotatedListeners()
            : MutableList<Pair<AbstractOrchestrateListener<out Any, out Any>, AbstractOrchestrateListener<out Any, out Any>?>> {
        return this.withIndex().map {
            val isFirst = it.index == 0
            val isLast =
                it.index == (this.size - 1 - COMMIT_LISTENER_PREFIX)

            val listener = it.value.first
            listener.isFirst = isFirst
            listener.isLast = isLast

            listener.withAnnotated() to it.value.second
        }.toMutableList()
    }

    private fun chainOrchestrateListeners(orchestrateListeners: List<Pair<AbstractOrchestrateListener<out Any, out Any>, AbstractOrchestrateListener<out Any, out Any>?>>) {
        var rollbackSequence = 0
        for (listenerWithIdx in orchestrateListeners.withIndex()) {
            val isFirst = listenerWithIdx.index == 0
            val isLast =
                listenerWithIdx.index == (orchestrateListeners.size - 1 - COMMIT_LISTENER_PREFIX)

            val listener = listenerWithIdx.value.first
            listenerWithIdx.value.second?.let {
                listener.isRollbackable = true
                rollbackSequence = it.orchestrateSequence
            }
            listener.rollbackSequence = rollbackSequence

            listener.isFirst = isFirst
            listener.isLast = isLast
            if (listenerWithIdx.index < orchestrateListeners.size - 1) {
                val nextListener = orchestrateListeners[listenerWithIdx.index + 1]
                listener.setNextOrchestrateListener(nextListener.first)
                nextListener.second?.let { listener.setNextRollbackOrchestrateListener(it) }
            }
        }
    }

    private fun chainRollbackListeners(orchestrateListeners: List<Pair<AbstractOrchestrateListener<out Any, out Any>, AbstractOrchestrateListener<out Any, out Any>?>>) {
        val rollbackListeners = orchestrateListeners.asSequence()
            .map { it.second }
            .filter { it != null }
            .toList()

        for (rollbackListenerWithIdx in rollbackListeners.withIndex()) {
            val rollbackListener = rollbackListenerWithIdx.value
                ?: throw IllegalStateException("Null Rollback listener occurred.")

            rollbackListener.isFirst = rollbackListenerWithIdx.index == 0
            rollbackListener.isLast = rollbackListenerWithIdx.index == (rollbackListeners.size - 1)

            if (rollbackListenerWithIdx.index > 0) {
                rollbackListener.beforeRollbackOrchestrateSequence =
                    rollbackListeners[rollbackListenerWithIdx.index - 1]!!.orchestrateSequence
            }
        }
    }

    private fun addDispatcher(orchestrateListeners: List<Pair<AbstractOrchestrateListener<out Any, out Any>, AbstractOrchestrateListener<out Any, out Any>?>>) {
        orchestrateListeners.forEach { (listener, rollbackListener) ->
            chainContainer.transactionDispatcher.addOrchestrate(listener)
            rollbackListener?.let { chainContainer.transactionDispatcher.addOrchestrate(it) }
        }
    }

    private fun <T : Any, V : Any> getMonoCommitOrchestrateListener(
        commandType: CommandType,
        function: TypeReified<T>,
    ) = MonoCommitOrchestrateListener(
        codec = chainContainer.codec,
        transactionManager = chainContainer.transactionManager,
        orchestratorId = orchestratorId,
        orchestrateSequence = orchestrateSequence + 1,
        monoOrchestrateCommand = MonoOrchestrateCommand<T, V>(
            commandType,
            chainContainer.codec,
            function
        ),
        resultHolder = chainContainer.resultHolder,
        requestHolder = chainContainer.requestHolder,
        typeReference = function.reified(),
    )

    private fun <T : Any, V : Any> getMonoRollbackOrchestrateListener(
        commandType: CommandType,
        rollback: TypeReified<T>?
    ) = rollback?.let {
        MonoRollbackOrchestrateListener<T, V>(
            codec = chainContainer.codec,
            transactionManager = chainContainer.transactionManager,
            orchestratorId = orchestratorId,
            orchestrateSequence = orchestrateSequence + 1,
            monoRollbackCommand = MonoRollbackCommand<T>(
                commandType,
                chainContainer.codec,
                it
            ),
            requestHolder = chainContainer.requestHolder,
            resultHolder = chainContainer.resultHolder,
            typeReference = it.reified(),
        )
    }

    internal class Pre<T : Any> internal constructor(
        private val orchestratorId: String,
        private val transactionManager: TransactionManager,
        private val transactionDispatcher: AbstractTransactionDispatcher,
        private val codec: Codec,
        private val resultHolder: ResultHolder,
        private val requestHolder: RequestHolder,
        private val orchestratorCache: OrchestratorCache,
    ) : OrchestrateChain.Pre<T> {

        override fun <V : Any> start(
            orchestrate: Orchestrate<T, V>,
            rollback: Rollback<T, *>?,
        ): DefaultOrchestrateChain<T, T, V> {
            val startOrchestrateListener =
                getStartOrchestrateListener<V>(CommandType.DEFAULT, orchestrate)
            val rollbackOrchestrateListener =
                getRollbackOrchestrateListener<V>(CommandType.DEFAULT, rollback)

            return DefaultOrchestrateChain(
                orchestratorId = orchestratorId,
                orchestrateSequence = 0,
                chainContainer = getStreamContainer(),
                orchestrateListener = startOrchestrateListener,
                rollbackOrchestrateListener = rollbackOrchestrateListener,
            )
        }

        override fun <V : Any> startWithContext(
            contextOrchestrate: ContextOrchestrate<T, V>,
            contextRollback: ContextRollback<T, *>?
        ): DefaultOrchestrateChain<T, T, V> {
            val startOrchestrateListener =
                getStartOrchestrateListener<V>(CommandType.CONTEXT, contextOrchestrate)
            val rollbackOrchestrateListener =
                getRollbackOrchestrateListener<V>(CommandType.CONTEXT, contextRollback)

            return DefaultOrchestrateChain(
                orchestratorId = orchestratorId,
                orchestrateSequence = 0,
                chainContainer = getStreamContainer(),
                orchestrateListener = startOrchestrateListener,
                rollbackOrchestrateListener = rollbackOrchestrateListener,
            )
        }

        private fun <V : Any> getStartOrchestrateListener(
            commandType: CommandType,
            function: TypeReified<T>,
        ) = StartOrchestrateListener(
            codec = codec,
            transactionManager = transactionManager,
            orchestratorId = orchestratorId,
            orchestrateSequence = 0,
            orchestrateCommand = OrchestrateCommand<T, V>(
                commandType,
                codec,
                function,
            ),
            requestHolder = requestHolder,
            resultHolder = resultHolder,
            typeReference = function.reified(),
        )

        private fun <V : Any> getRollbackOrchestrateListener(
            commandType: CommandType,
            rollback: TypeReified<T>?
        ) = rollback?.let {
            RollbackOrchestrateListener<T, V>(
                codec = codec,
                transactionManager = transactionManager,
                orchestratorId = orchestratorId,
                orchestrateSequence = 0,
                rollbackCommand = RollbackCommand<T>(
                    commandType,
                    codec,
                    it
                ),
                requestHolder = requestHolder,
                resultHolder = resultHolder,
                typeReference = it.reified()
            )
        }

        override fun <V : Any> startReactive(
            orchestrate: Orchestrate<T, Mono<V>>,
            rollback: Rollback<T, Mono<*>>?,
        ): DefaultOrchestrateChain<T, T, V> {
            val startOrchestrateListener =
                getMonoStartOrchestrateListener<V>(CommandType.DEFAULT, orchestrate)
            val rollbackOrchestrateListener =
                getMonoRollbackOrchestrateListener<V>(CommandType.DEFAULT, rollback)

            return DefaultOrchestrateChain(
                orchestratorId = orchestratorId,
                orchestrateSequence = 0,
                chainContainer = getStreamContainer(),
                orchestrateListener = startOrchestrateListener,
                rollbackOrchestrateListener = rollbackOrchestrateListener,
            )
        }

        override fun <V : Any> startReactiveWithContext(
            contextOrchestrate: ContextOrchestrate<T, Mono<V>>,
            contextRollback: ContextRollback<T, Mono<*>>?
        ): DefaultOrchestrateChain<T, T, V> {
            val startOrchestrateListener =
                getMonoStartOrchestrateListener<V>(CommandType.CONTEXT, contextOrchestrate)
            val rollbackOrchestrateListener =
                getMonoRollbackOrchestrateListener<V>(CommandType.CONTEXT, contextRollback)

            return DefaultOrchestrateChain(
                orchestratorId = orchestratorId,
                orchestrateSequence = 0,
                chainContainer = getStreamContainer(),
                orchestrateListener = startOrchestrateListener,
                rollbackOrchestrateListener = rollbackOrchestrateListener,
            )
        }

        private fun <V : Any> getMonoStartOrchestrateListener(
            commandType: CommandType,
            function: TypeReified<T>,
        ) = MonoStartOrchestrateListener(
            codec = codec,
            transactionManager = transactionManager,
            orchestratorId = orchestratorId,
            orchestrateSequence = 0,
            monoOrchestrateCommand = MonoOrchestrateCommand<T, V>(
                commandType,
                codec,
                function,
            ),
            requestHolder = requestHolder,
            resultHolder = resultHolder,
            typeReference = function.reified(),
        )

        private fun <V : Any> getMonoRollbackOrchestrateListener(
            commandType: CommandType,
            rollback: TypeReified<T>?,
        ) = rollback?.let {
            MonoRollbackOrchestrateListener<T, V>(
                codec = codec,
                transactionManager = transactionManager,
                orchestratorId = orchestratorId,
                orchestrateSequence = 0,
                monoRollbackCommand = MonoRollbackCommand(
                    commandType,
                    codec,
                    it,
                ),
                requestHolder = requestHolder,
                resultHolder = resultHolder,
                typeReference = it.reified(),
            )
        }

        private fun getStreamContainer(): ChainContainer = ChainContainer(
            transactionManager,
            transactionDispatcher,
            codec,
            resultHolder,
            requestHolder,
            orchestratorCache,
        )
    }

    companion object {
        private const val COMMIT_LISTENER_PREFIX = 1
    }

    private data class ChainContainer(
        val transactionManager: TransactionManager,
        val transactionDispatcher: AbstractTransactionDispatcher,
        val codec: Codec,
        val resultHolder: ResultHolder,
        val requestHolder: RequestHolder,
        val orchestratorCache: OrchestratorCache,
    )
}

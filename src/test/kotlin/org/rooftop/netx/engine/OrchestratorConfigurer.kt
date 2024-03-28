package org.rooftop.netx.engine

import org.rooftop.netx.api.Orchestrate
import org.rooftop.netx.api.Orchestrator
import org.rooftop.netx.api.Rollback
import org.rooftop.netx.api.TypeReference
import org.rooftop.netx.engine.OrchestratorTest.Companion.contextResult
import org.rooftop.netx.engine.OrchestratorTest.Companion.monoRollbackResult
import org.rooftop.netx.engine.OrchestratorTest.Companion.rollbackOrchestratorResult
import org.rooftop.netx.engine.OrchestratorTest.Companion.upChainResult
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import reactor.core.publisher.Mono
import java.time.Instant

@Configuration
class OrchestratorConfigurer(
    private val orchestratorFactory: OrchestratorFactory,
) {

    @Bean(name = ["numberOrchestrator"])
    fun numberOrchestrator(): Orchestrator<Int, Int> {
        return orchestratorFactory.create<Int>("numberOrchestrator")
            .start(orchestrate = { it + 1 })
            .join(orchestrate = { it + 1 })
            .joinReactive(orchestrate = { Mono.just(it + 1) })
            .commit(orchestrate = { it + 1 })
    }

    @Bean(name = ["homeOrchestrator"])
    fun homeOrchestrator(): Orchestrator<OrchestratorTest.Home, OrchestratorTest.Home> {
        return orchestratorFactory.create<OrchestratorTest.Home>("homeOrchestrator")
            .startReactive({ home ->
                Mono.fromCallable {
                    home.addPerson(OrchestratorTest.Person("Mother"))
                    home
                }
            })
            .join({
                it.addPerson(OrchestratorTest.Person("Father"))
                it
            })
            .commitReactive({ home ->
                Mono.fromCallable {
                    home.addPerson(OrchestratorTest.Person("Son"))
                    home
                }
            })
    }

    @Bean(name = ["instantOrchestrator"])
    fun instantOrchestrator(): Orchestrator<OrchestratorTest.InstantWrapper, OrchestratorTest.InstantWrapper> {
        return orchestratorFactory.create<OrchestratorTest.InstantWrapper>("instantOrchestrator")
            .start({ it })
            .commit({ it })
    }

    @Bean(name = ["manyTypeOrchestrator"])
    fun manyTypeOrchestrator(): Orchestrator<Int, OrchestratorTest.Home> {
        return orchestratorFactory.create<Int>("manyTypeOrchestrator")
            .start({ "String" })
            .join({ 1L })
            .join({ 0.1 })
            .join({ OrchestratorTest.InstantWrapper(Instant.now()) })
            .commit({ OrchestratorTest.Home("HOME", mutableListOf()) })
    }

    @Bean(name = ["rollbackOrchestrator"])
    fun rollbackOrchestrator(): Orchestrator<String, String> {
        return orchestratorFactory.create<String>("rollbackOrchestrator")
            .start(
                orchestrate = {
                    rollbackOrchestratorResult.add("1")
                },
                rollback = {
                    rollbackOrchestratorResult.add("-1")
                }
            )
            .join(
                orchestrate = {
                    rollbackOrchestratorResult.add("2")
                }
            )
            .join(
                orchestrate = {
                    rollbackOrchestratorResult.add("3")
                },
                rollback = { rollbackOrchestratorResult.add("-3") }
            )
            .commit(
                orchestrate = {
                    rollbackOrchestratorResult.add("4")
                    throw IllegalArgumentException("Rollback")
                },
                rollback = {
                    rollbackOrchestratorResult.add("-4")
                }
            )
    }

    @Bean(name = ["upChainRollbackOrchestrator"])
    fun upChainRollbackOrchestrator(): Orchestrator<String, String> {
        return orchestratorFactory.create<String>("upChainRollbackOrchestrator")
            .start({ upChainResult.add("1") }, { upChainResult.add("-1") })
            .join({ upChainResult.add("2") })
            .join({ upChainResult.add("3") }, { upChainResult.add("-3") })
            .commit({
                upChainResult.add("4")
                throw IllegalArgumentException("Rollback for test")
            })
    }

    @Bean(name = ["monoRollbackOrchestrator"])
    fun monoRollbackOrchestrator(): Orchestrator<String, String> {
        return orchestratorFactory.create<String>("monoRollbackOrchestrator")
            .startReactive(
                { Mono.fromCallable { monoRollbackResult.add("1") } },
                { Mono.fromCallable { monoRollbackResult.add("-1") } }
            )
            .joinReactive({ Mono.fromCallable { monoRollbackResult.add("2") } })
            .joinReactive(
                { Mono.fromCallable { monoRollbackResult.add("3") } },
                { Mono.fromCallable { monoRollbackResult.add("-3") } }
            )
            .commitReactive({
                Mono.fromCallable {
                    monoRollbackResult.add("4")
                    throw IllegalArgumentException("Rollback for test")
                }
            })
    }

    @Bean(name = ["contextOrchestrator"])
    fun contextOrchestrator(): Orchestrator<String, String> {
        return orchestratorFactory.create<String>("contextOrchestrator")
            .startWithContext(
                contextOrchestrate = { context, request ->
                    context.set("start-1", request)
                    "1"
                },
                contextRollback = { context, _ ->
                    val start1 = context.decodeContext("start-1", String::class)
                    val join2 = context.decodeContext("join-2", String::class)
                    val join3 = context.decodeContext("join-3", String::class)
                    val rCommit4 = context.decodeContext("r-commit-4", String::class)
                    val rJoin3 = context.decodeContext("r-join-3", String::class)

                    contextResult.addAll(listOf(start1, join2, join3, rCommit4, rJoin3))
                }
            )
            .joinWithContext(
                contextOrchestrate = { context, request ->
                    context.set("join-2", request)
                    "2"
                }
            )
            .joinReactiveWithContext(
                contextOrchestrate = { context, request ->
                    Mono.fromCallable {
                        context.set("join-3", request)
                        "3"
                    }
                },
                contextRollback = { context, request ->
                    Mono.fromCallable {
                        context.set("r-join-3", "r$request")
                    }
                }
            )
            .commitWithContext(
                contextOrchestrate = { context, request ->
                    context.set("commit-4", request)
                    throw IllegalArgumentException("Rollback")
                },
                contextRollback = { context, request ->
                    context.set("r-commit-4", "r$request")
                }
            )
    }

    @Bean(name = ["pairOrchestrator"])
    fun pairOrchestrator(): Orchestrator<String, Pair<OrchestratorTest.Foo, OrchestratorTest.Foo>> {
        return orchestratorFactory.create<String>("pairOrchestrator")
            .start({ OrchestratorTest.Foo(it) to OrchestratorTest.Foo(it) })
            .join(PairOrchestrate, PairRollback)
            .joinReactive(MonoPairOrchestrate, MonoPairRollback)
            .commit(object :
                Orchestrate<Pair<OrchestratorTest.Foo, OrchestratorTest.Foo>, Pair<OrchestratorTest.Foo, OrchestratorTest.Foo>> {
                override fun orchestrate(request: Pair<OrchestratorTest.Foo, OrchestratorTest.Foo>): Pair<OrchestratorTest.Foo, OrchestratorTest.Foo> {
                    throw IllegalArgumentException("Rollback")
                }

                override fun reified(): TypeReference<Pair<OrchestratorTest.Foo, OrchestratorTest.Foo>> {
                    return object :
                        TypeReference<Pair<OrchestratorTest.Foo, OrchestratorTest.Foo>>() {}
                }
            })
    }

    @Bean(name = ["startWithContextOrchestrator"])
    fun startWithContextOrchestrator(): Orchestrator<String, String> {
        return orchestratorFactory.create<String>("startWithContextOrchestrator")
            .startWithContext({ context, _ ->
                context.decodeContext("key", String::class)
            })
            .commitWithContext({ context, _ ->
                context.decodeContext("key", String::class)
            })
    }

    @Bean(name = ["fooContextOrchestrator"])
    fun fooContextOrchestrator(): Orchestrator<String, List<OrchestratorTest.Foo>> {
        return orchestratorFactory.create<String>("fooContextOrchestrator")
            .startWithContext({ context, _ ->
                val before = context.decodeContext("0", OrchestratorTest.Foo::class)
                context.set("1", OrchestratorTest.Foo("startWithContext"))
            })
            .joinWithContext({ context, _ ->
                val before = context.decodeContext("1", OrchestratorTest.Foo::class)
                context.set("2", OrchestratorTest.Foo("joinWithContext"))
            })
            .commitWithContext({ context, _ ->
                val before = context.decodeContext("2", OrchestratorTest.Foo::class)
                listOf(
                    context.decodeContext("0", OrchestratorTest.Foo::class),
                    context.decodeContext("1", OrchestratorTest.Foo::class),
                    context.decodeContext("2", OrchestratorTest.Foo::class),
                )
            })
    }

    object PairOrchestrate :
        Orchestrate<Pair<OrchestratorTest.Foo, OrchestratorTest.Foo>, Pair<OrchestratorTest.Foo, OrchestratorTest.Foo>> {
        override fun orchestrate(request: Pair<OrchestratorTest.Foo, OrchestratorTest.Foo>): Pair<OrchestratorTest.Foo, OrchestratorTest.Foo> {
            return OrchestratorTest.Foo(request.first.name) to OrchestratorTest.Foo(request.first.name)
        }

        override fun reified(): TypeReference<Pair<OrchestratorTest.Foo, OrchestratorTest.Foo>> {
            return object : TypeReference<Pair<OrchestratorTest.Foo, OrchestratorTest.Foo>>() {}
        }
    }

    object PairRollback :
        Rollback<Pair<OrchestratorTest.Foo, OrchestratorTest.Foo>, Pair<OrchestratorTest.Foo, OrchestratorTest.Foo>> {
        override fun rollback(request: Pair<OrchestratorTest.Foo, OrchestratorTest.Foo>): Pair<OrchestratorTest.Foo, OrchestratorTest.Foo> {
            return OrchestratorTest.Foo(request.first.name) to OrchestratorTest.Foo(request.first.name)
        }

        override fun reified(): TypeReference<Pair<OrchestratorTest.Foo, OrchestratorTest.Foo>> {
            return object : TypeReference<Pair<OrchestratorTest.Foo, OrchestratorTest.Foo>>() {}
        }
    }

    object MonoPairOrchestrate :
        Orchestrate<Pair<OrchestratorTest.Foo, OrchestratorTest.Foo>, Mono<Pair<OrchestratorTest.Foo, OrchestratorTest.Foo>>> {

        override fun orchestrate(request: Pair<OrchestratorTest.Foo, OrchestratorTest.Foo>): Mono<Pair<OrchestratorTest.Foo, OrchestratorTest.Foo>> {
            return Mono.fromCallable {
                OrchestratorTest.Foo(request.first.name) to OrchestratorTest.Foo(
                    request.first.name
                )
            }
        }

        override fun reified(): TypeReference<Pair<OrchestratorTest.Foo, OrchestratorTest.Foo>> {
            return object : TypeReference<Pair<OrchestratorTest.Foo, OrchestratorTest.Foo>>() {}
        }
    }

    object MonoPairRollback :
        Rollback<Pair<OrchestratorTest.Foo, OrchestratorTest.Foo>, Mono<*>> {
        override fun rollback(request: Pair<OrchestratorTest.Foo, OrchestratorTest.Foo>): Mono<Pair<OrchestratorTest.Foo, OrchestratorTest.Foo>> {
            return Mono.fromCallable {
                OrchestratorTest.Foo(request.first.name) to OrchestratorTest.Foo(
                    request.first.name
                )
            }
        }

        override fun reified(): TypeReference<Pair<OrchestratorTest.Foo, OrchestratorTest.Foo>> {
            return object : TypeReference<Pair<OrchestratorTest.Foo, OrchestratorTest.Foo>>() {}
        }
    }
}

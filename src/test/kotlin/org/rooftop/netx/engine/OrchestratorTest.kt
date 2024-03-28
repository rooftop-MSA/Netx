package org.rooftop.netx.engine

import io.kotest.assertions.nondeterministic.eventually
import io.kotest.assertions.throwables.shouldThrowWithMessage
import io.kotest.core.annotation.DisplayName
import io.kotest.core.spec.style.DescribeSpec
import io.kotest.matchers.equality.shouldBeEqualToComparingFields
import io.kotest.matchers.equals.shouldBeEqual
import org.rooftop.netx.api.Orchestrator
import org.rooftop.netx.api.TypeReference
import org.rooftop.netx.meta.EnableDistributedTransaction
import org.rooftop.netx.redis.RedisContainer
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.TestPropertySource
import java.time.Instant
import kotlin.time.Duration.Companion.seconds

@EnableDistributedTransaction
@ContextConfiguration(
    classes = [
        RedisContainer::class,
        OrchestratorConfigurer::class,
    ]
)
@DisplayName("Orchestrator 클래스의")
@TestPropertySource("classpath:application.properties")
class OrchestratorTest(
    private val numberOrchestrator: Orchestrator<Int, Int>,
    private val homeOrchestrator: Orchestrator<Home, Home>,
    private val instantOrchestrator: Orchestrator<InstantWrapper, InstantWrapper>,
    private val manyTypeOrchestrator: Orchestrator<Int, Home>,
    @Qualifier("rollbackOrchestrator") private val rollbackOrchestrator: Orchestrator<String, String>,
    @Qualifier("upChainRollbackOrchestrator") private val upChainRollbackOrchestrator: Orchestrator<String, String>,
    @Qualifier("monoRollbackOrchestrator") private val monoRollbackOrchestrator: Orchestrator<String, String>,
    @Qualifier("contextOrchestrator") private val contextOrchestrator: Orchestrator<String, String>,
    @Qualifier("pairOrchestrator") private val pairOrchestrator: Orchestrator<String, Pair<Foo, Foo>>,
    @Qualifier("startWithContextOrchestrator") private val startWithContextOrchestrator: Orchestrator<String, String>,
    @Qualifier("fooContextOrchestrator") private val fooContextOrchestrator: Orchestrator<String, List<Foo>>
) : DescribeSpec({

    describe("numberOrchestrator 구현채는") {
        context("transaction 메소드가 호출되면,") {
            it("처음 입력받은 숫자에 orchestrate만큼의 수를 더한다.") {
                val result = numberOrchestrator.transactionSync(3)

                result.isSuccess shouldBeEqual true
                result.decodeResult(Int::class) shouldBeEqual 7
            }
        }
    }

    describe("homeOrchestrator 구현채는") {
        val expected = Home(
            "Korea, Seoul, Gangnam",
            mutableListOf(Person("Mother"), Person("Father"), Person("Son"))
        )

        context("transaction 메소드가 호출되면,") {
            it("처음 입력 받은 Home에 Mother, Father, Son을 추가한다.") {
                val result =
                    homeOrchestrator.transaction(Home("Korea, Seoul, Gangnam", mutableListOf()))
                        .block()

                result!!.isSuccess shouldBeEqual true
                result.decodeResult(Home::class) shouldBeEqualToComparingFields expected
            }
        }
    }

    describe("instantOrchestrator 구현채는") {
        val expected = InstantWrapper(Instant.now())

        context("transaction 메소드가 호출되면,") {
            it("처음 입력받은 instantWrapper를 그대로 반환한다.") {
                val result = instantOrchestrator.transactionSync(expected)

                result.isSuccess shouldBeEqual true
                result.decodeResult(InstantWrapper::class) shouldBeEqualToComparingFields expected
            }
        }
    }

    describe("manyTypeOrchestrator 구현채는") {
        val expected = Home("HOME", mutableListOf())

        context("transaction메소드가 호출되면,") {
            it("처음 Home을 반환한다.") {
                val result = manyTypeOrchestrator.transactionSync(1)

                result.isSuccess shouldBeEqual true
                result.decodeResult(Home::class) shouldBeEqualToComparingFields expected
            }
        }
    }

    describe("rollbackOrchestrator 구현채는") {
        val expected = listOf("1", "2", "3", "4", "-4", "-3", "-1")

        context("transaction 메소드가 호출되면,") {
            it("실패한 부분부터 위로 거슬러 올라가며 롤백한다") {
                val result = rollbackOrchestrator.transactionSync("")

                result.isSuccess shouldBeEqual false
                shouldThrowWithMessage<IllegalArgumentException>("Rollback") {
                    result.throwError()
                }
                eventually(5.seconds) {
                    rollbackOrchestratorResult shouldBeEqual expected
                }
            }
        }
    }

    describe("upStreamRollbackOrchestrator 구현채는") {
        val expected = listOf("1", "2", "3", "4", "-3", "-1")

        it("호출할 rollback function이 없으면, 가장 가까운 상단의 rollback을 호출한다.") {
            val result = upChainRollbackOrchestrator.transactionSync("")

            result.isSuccess shouldBeEqual false
            shouldThrowWithMessage<IllegalArgumentException>("Rollback for test") {
                result.throwError()
            }
            eventually(5.seconds) {
                upChainResult shouldBeEqual expected
            }
        }
    }

    describe("monoRollbackOrchestrator 구현채는") {
        context("transaction 메소드가 호출되면,") {
            val expected = listOf("1", "2", "3", "4", "-3", "-1")

            it("실패한 부분부터 위로 거슬러 올라가며 롤백한다.") {
                val result = monoRollbackOrchestrator.transactionSync("")

                result.isSuccess shouldBeEqual false
                shouldThrowWithMessage<IllegalArgumentException>("Rollback for test") {
                    result.throwError()
                }
                eventually(5.seconds) {
                    monoRollbackResult shouldBeEqual expected
                }
            }
        }
    }

    describe("contextOrchestrator 구현채는") {
        context("transaction 메소드가 호출되면,") {
            val expected = listOf("0", "1", "2", "r3", "r2")

            it("context 에서 아이템을 교환하며 Saga를 진행한다.") {
                val result = contextOrchestrator.transactionSync("0")

                result.isSuccess shouldBeEqual false
                shouldThrowWithMessage<IllegalArgumentException>("Rollback") {
                    result.throwError()
                }
                eventually(5.seconds) {
                    contextResult shouldBeEqual expected
                }
            }
        }
    }

    describe("pairOrchestrator 구현채는") {
        context("transaction 메소드가 호출되면,") {
            it("입력받은 파라미터를 name 으로 갖는 Foo pair 를 반환한다. ") {
                val result = pairOrchestrator.transactionSync("james")

                result.isSuccess shouldBeEqual false
                shouldThrowWithMessage<IllegalArgumentException>("Rollback") {
                    result.throwError()
                }
            }
        }
    }

    describe("startWithContextOrchestrator 구현채는") {
        context("context와 함께 transaction 메소드가 호출되면,") {
            it("key에 해당하는 context를 반환한다.") {
                val result = startWithContextOrchestrator.transactionSync(
                    "ignored request",
                    mutableMapOf("key" to "hello")
                )

                result.decodeResultOrThrow(String::class) shouldBeEqual "hello"
            }
        }
    }

    describe("fooContextOrchestrator 구현채는") {
        context("context 와 함께 transaction 메소드가 호출되면,") {
            val expected = listOf(
                Foo("startSync"),
                Foo("startWithContext"),
                Foo("joinWithContext"),
            )

            it("0,1,2 Foo가 들어있는 Foo list를 반환한다.") {
                val result = fooContextOrchestrator.transactionSync(
                    "",
                    mutableMapOf("0" to Foo("startSync"))
                )

                result.isSuccess shouldBeEqual true
                result.decodeResultOrThrow(object :
                    TypeReference<List<Foo>>() {}) shouldBeEqual expected
            }
        }
    }
}) {
    data class Home(
        val address: String,
        val persons: MutableList<Person>
    ) {
        fun addPerson(person: Person) {
            persons.add(person)
        }
    }

    data class Person(val name: String)

    data class InstantWrapper(
        val time: Instant,
    )

    data class Foo(val name: String)

    companion object {
        val rollbackOrchestratorResult = mutableListOf<String>()
        val upChainResult = mutableListOf<String>()
        val monoRollbackResult = mutableListOf<String>()
        val contextResult = mutableListOf<String>()
    }
}

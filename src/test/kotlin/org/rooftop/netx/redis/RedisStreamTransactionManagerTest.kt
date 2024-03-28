package org.rooftop.netx.redis

import io.kotest.assertions.nondeterministic.eventually
import io.kotest.assertions.throwables.shouldThrowMessage
import io.kotest.core.annotation.DisplayName
import io.kotest.core.spec.style.DescribeSpec
import io.kotest.matchers.shouldBe
import org.rooftop.netx.api.*
import org.rooftop.netx.meta.EnableDistributedTransaction
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.TestPropertySource
import reactor.test.StepVerifier
import kotlin.time.Duration.Companion.seconds

@EnableDistributedTransaction
@ContextConfiguration(
    classes = [
        RedisContainer::class,
        MonoTransactionHandlerAssertions::class,
        NoPublisherTransactionHandlerAssertions::class,
    ]
)
@DisplayName("RedisStreamTransactionManager 클래스의")
@TestPropertySource("classpath:application.properties")
internal class RedisStreamTransactionManagerTest(
    private val transactionManager: TransactionManager,
    private val monoTransactionHandlerAssertions: MonoTransactionHandlerAssertions,
    private val noPublisherTransactionHandlerAssertions: NoPublisherTransactionHandlerAssertions,
) : DescribeSpec({

    beforeEach {
        monoTransactionHandlerAssertions.clear()
        noPublisherTransactionHandlerAssertions.clear()
    }

    describe("start 메소드는") {
        context("어떤 event도 없이 호출되면,") {
            it("트랜잭션을 시작하고 transaction-id를 반환한다.") {
                transactionManager.start().subscribe()

                eventually(5.seconds) {
                    monoTransactionHandlerAssertions.startCountShouldBe(1)
                    noPublisherTransactionHandlerAssertions.startCountShouldBe(1)
                }
            }
        }

        context("서로 다른 id의 트랜잭션이 여러번 시작되어도") {
            it("모두 읽을 수 있다.") {
                transactionManager.start().block()
                transactionManager.start().block()

                eventually(5.seconds) {
                    monoTransactionHandlerAssertions.startCountShouldBe(2)
                    noPublisherTransactionHandlerAssertions.startCountShouldBe(2)
                }
            }
        }
    }

    describe("syncStart 메소드는") {
        context("어떤 event도 없이 호출되면,") {
            it("트랜잭션을 시작하고 transaction-id를 반환한다.") {
                transactionManager.syncStart()

                eventually(5.seconds) {
                    monoTransactionHandlerAssertions.startCountShouldBe(1)
                    noPublisherTransactionHandlerAssertions.startCountShouldBe(1)
                }
            }
        }

        context("서로 다른 id의 트랜잭션이 여러번 시작되어도") {
            it("모두 읽을 수 있다.") {
                transactionManager.syncStart()
                transactionManager.syncStart()

                eventually(5.seconds) {
                    monoTransactionHandlerAssertions.startCountShouldBe(2)
                    noPublisherTransactionHandlerAssertions.startCountShouldBe(2)
                }
            }
        }
    }

    describe("join 메소드는") {
        context("존재하는 transactionId를 입력받으면,") {
            val transactionId = transactionManager.start().block()!!

            it("트랜잭션에 참여한다.") {
                transactionManager.join(transactionId).subscribe()

                eventually(5.seconds) {
                    monoTransactionHandlerAssertions.joinCountShouldBe(1)
                    noPublisherTransactionHandlerAssertions.joinCountShouldBe(1)
                }
            }
        }

        context("존재하지 않는 transactionId를 입력받으면,") {
            it("TransactionException 을 던진다.") {
                val result = transactionManager.join(NOT_EXIST_TX_ID)

                StepVerifier.create(result)
                    .verifyErrorMessage("Cannot find exists transaction id \"$NOT_EXIST_TX_ID\"")
            }
        }
    }

    describe("syncJoin 메소드는") {
        context("존재하는 transactionId를 입력받으면,") {
            val transactionId = transactionManager.syncStart()

            it("트랜잭션에 참여한다.") {
                transactionManager.syncJoin(transactionId)

                eventually(5.seconds) {
                    monoTransactionHandlerAssertions.joinCountShouldBe(1)
                    noPublisherTransactionHandlerAssertions.joinCountShouldBe(1)
                }
            }
        }

        context("존재하지 않는 transactionId를 입력받으면,") {
            it("TransactionException 을 던진다.") {
                shouldThrowMessage("Cannot find exists transaction id \"$NOT_EXIST_TX_ID\"") {
                    transactionManager.syncJoin(NOT_EXIST_TX_ID)
                }
            }
        }
    }

    describe("exists 메소드는") {
        context("존재하는 transactionId를 입력받으면,") {
            val transactionId = transactionManager.start().block()!!

            it("트랜잭션 id를 반환한다.") {
                val result = transactionManager.exists(transactionId)

                StepVerifier.create(result)
                    .expectNext(transactionId)
                    .verifyComplete()
            }
        }

        context("존재하지 않는 transactionId를 입력받으면,") {
            it("TransactionException 을 던진다.") {
                val result = transactionManager.exists(NOT_EXIST_TX_ID)

                StepVerifier.create(result)
                    .verifyErrorMessage("Cannot find exists transaction id \"$NOT_EXIST_TX_ID\"")
            }
        }
    }

    describe("syncExists 메소드는") {
        context("존재하는 transactionId를 입력받으면,") {
            val transactionId = transactionManager.syncStart()

            it("트랜잭션 id를 반환한다.") {
                val result = transactionManager.syncExists(transactionId)

                result shouldBe transactionId
            }
        }

        context("존재하지 않는 transactionId를 입력받으면,") {
            it("TransactionException 을 던진다.") {
                shouldThrowMessage("Cannot find exists transaction id \"$NOT_EXIST_TX_ID\"") {
                    transactionManager.syncExists(NOT_EXIST_TX_ID)
                }
            }
        }
    }

    describe("commit 메소드는") {
        context("존재하는 transactionId를 입력받으면,") {
            val transactionId = transactionManager.start().block()!!

            it("commit 메시지를 publish 한다") {
                transactionManager.commit(transactionId).block()

                eventually(5.seconds) {
                    monoTransactionHandlerAssertions.commitCountShouldBe(1)
                    noPublisherTransactionHandlerAssertions.commitCountShouldBe(1)
                }
            }
        }

        context("존재하지 않는 transactionId를 입력받으면,") {
            it("TransactionException 을 던진다.") {
                val result = transactionManager.commit(NOT_EXIST_TX_ID)

                StepVerifier.create(result)
                    .verifyErrorMessage("Cannot find exists transaction id \"$NOT_EXIST_TX_ID\"")
            }
        }
    }

    describe("syncCommit 메소드는") {
        context("존재하는 transactionId를 입력받으면,") {
            val transactionId = transactionManager.syncStart()

            it("commit 메시지를 publish 한다") {
                transactionManager.syncCommit(transactionId)

                eventually(5.seconds) {
                    monoTransactionHandlerAssertions.commitCountShouldBe(1)
                    noPublisherTransactionHandlerAssertions.commitCountShouldBe(1)
                }
            }
        }

        context("존재하지 않는 transactionId를 입력받으면,") {
            it("TransactionException 을 던진다.") {
                shouldThrowMessage("Cannot find exists transaction id \"$NOT_EXIST_TX_ID\"") {
                    transactionManager.syncCommit(NOT_EXIST_TX_ID)
                }
            }
        }
    }

    describe("rollback 메소드는") {
        context("존재하는 transactionId를 입력받으면,") {
            val transactionId = transactionManager.start().block()!!

            it("rollback 메시지를 publish 한다") {
                transactionManager.rollback(transactionId, "rollback for test").block()

                eventually(5.seconds) {
                    monoTransactionHandlerAssertions.rollbackCountShouldBe(1)
                    noPublisherTransactionHandlerAssertions.rollbackCountShouldBe(1)
                }
            }
        }

        context("존재하지 않는 transactionId를 입력받으면,") {
            it("TransactionException 을 던진다.") {
                val result = transactionManager.rollback(NOT_EXIST_TX_ID, "rollback for test")

                StepVerifier.create(result)
                    .verifyErrorMessage("Cannot find exists transaction id \"$NOT_EXIST_TX_ID\"")
            }
        }
    }

    describe("syncRollback 메소드는") {
        context("존재하는 transactionId를 입력받으면,") {
            val transactionId = transactionManager.syncStart()

            it("rollback 메시지를 publish 한다") {
                transactionManager.syncRollback(transactionId, "rollback for test")

                eventually(5.seconds) {
                    monoTransactionHandlerAssertions.rollbackCountShouldBe(1)
                    noPublisherTransactionHandlerAssertions.rollbackCountShouldBe(1)
                }
            }
        }

        context("존재하지 않는 transactionId를 입력받으면,") {
            it("TransactionException 을 던진다.") {
                shouldThrowMessage("Cannot find exists transaction id \"$NOT_EXIST_TX_ID\"") {
                    transactionManager.syncRollback(NOT_EXIST_TX_ID, "rollback for test")
                }
            }
        }
    }
}) {

    private companion object {
        private const val NOT_EXIST_TX_ID = "NOT_EXISTS_TX_ID"
    }
}

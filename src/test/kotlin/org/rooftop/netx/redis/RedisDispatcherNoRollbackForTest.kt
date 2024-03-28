package org.rooftop.netx.redis

import io.kotest.assertions.nondeterministic.eventually
import io.kotest.core.annotation.DisplayName
import io.kotest.core.spec.style.StringSpec
import org.rooftop.netx.api.TransactionManager
import org.rooftop.netx.meta.EnableDistributedTransaction
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.TestPropertySource
import kotlin.time.Duration.Companion.seconds

@EnableDistributedTransaction
@ContextConfiguration(
    classes = [
        RedisContainer::class,
        TransactionNoRollbackForStorage::class,
        MonoTransactionHandlerAssertions::class,
    ]
)
@DisplayName("RedisDispatcherNoRollbackForTest")
@TestPropertySource("classpath:application.properties")
internal class RedisDispatcherNoRollbackForTest(
    private val transactionAssertions: MonoTransactionHandlerAssertions,
    private val transactionManager: TransactionManager,
) : StringSpec({

    beforeEach {
        transactionAssertions.clear()
    }

    "noRollbackFor로 IllegalArgumentException이 걸려있으면, 해당 예외가 발생해도 rollback 하지 않는다." {
        transactionManager.syncStart(IllegalArgumentExceptionEvent("illegal"))

        eventually(5.seconds) {
            transactionAssertions.startCountShouldBe(1)
            transactionAssertions.rollbackCountShouldBe(0)
        }
    }

    "noRollbackFor로 UnSupportedOperationException이 걸려있으면, 해당 예외가 발생해도 rollback 하지않는다." {
        transactionManager.syncStart(UnSupportedOperationExceptionEvent("unsupports"))

        eventually(5.seconds) {
            transactionAssertions.startCountShouldBe(1)
            transactionAssertions.rollbackCountShouldBe(0)
        }
    }

    "noRollbackFor에 설정되지 않은 예외가 발생하면, rollback을 수행한다." {
        transactionManager.syncStart(NoSuchElementExceptionEvent("noSuchElement"))

        eventually(5.seconds) {
            transactionAssertions.startCountShouldBe(1)
            transactionAssertions.rollbackCountShouldBe(1)
        }
    }
}) {

    class IllegalArgumentExceptionEvent(val illegalArgumentException: String)
    class UnSupportedOperationExceptionEvent(val unsupportedOperationException: String)
    class NoSuchElementExceptionEvent(val noSuchElementException: String)
}

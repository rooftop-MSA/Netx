package org.rooftop.netx.client

import io.kotest.assertions.nondeterministic.eventually
import io.kotest.core.annotation.DisplayName
import io.kotest.core.spec.style.FunSpec
import io.kotest.data.forAll
import io.kotest.data.row
import org.rooftop.netx.api.TransactionManager
import org.rooftop.netx.meta.EnableDistributedTransaction
import org.rooftop.netx.redis.RedisContainer
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.TestPropertySource
import kotlin.time.Duration.Companion.minutes

@DisplayName("Netx 부하테스트")
@SpringBootTest(
    classes = [
        RedisContainer::class,
        LoadRunner::class,
        TransactionReceiveStorage::class,
    ]
)
@EnableDistributedTransaction
@TestPropertySource("classpath:fast-recover-mode.properties")
internal class NetxLoadTest(
    private val transactionManager: TransactionManager,
    private val loadRunner: LoadRunner,
    private val transactionReceiveStorage: TransactionReceiveStorage,
) : FunSpec({

    test("Netx는 부하가 가중되어도, 결과적 일관성을 보장한다.") {
        forAll(
            row(1, 1),
            row(10, 10),
            row(100, 100),
            row(1_000, 1_000),
            row(10_000, 10_000),
        ) { commitLoadCount, rollbackLoadCount ->
            transactionReceiveStorage.clear()

            loadRunner.load(commitLoadCount) {
                transactionManager.start(LoadTestEvent(NO_ROLLBACK)).block()!!
            }

            loadRunner.load(rollbackLoadCount) {
                transactionManager.start(LoadTestEvent(ROLLBACK)).block()!!
            }

            eventually(3.minutes) {
                transactionReceiveStorage.startCountShouldBeGreaterThanOrEqual(commitLoadCount + rollbackLoadCount)
                transactionReceiveStorage.joinCountShouldBeGreaterThanOrEqual(commitLoadCount + rollbackLoadCount)
                transactionReceiveStorage.commitCountShouldBeGreaterThanOrEqual(commitLoadCount + rollbackLoadCount)
                transactionReceiveStorage.rollbackCountShouldBeGreaterThanOrEqual(rollbackLoadCount)
            }
        }
    }

}) {
    data class LoadTestEvent(val load: String)

    private companion object {
        private const val ROLLBACK = "-"
        private const val NO_ROLLBACK = "+"
    }
}

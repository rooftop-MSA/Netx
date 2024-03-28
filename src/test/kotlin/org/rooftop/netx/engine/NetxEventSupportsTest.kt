package org.rooftop.netx.engine

import io.kotest.core.annotation.DisplayName
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.equality.shouldBeEqualUsingFields
import io.kotest.matchers.equals.shouldBeEqual
import org.rooftop.netx.api.TransactionManager
import org.rooftop.netx.meta.EnableDistributedTransaction
import org.rooftop.netx.redis.RedisContainer
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.TestPropertySource

@EnableDistributedTransaction
@ContextConfiguration(
    classes = [
        RedisContainer::class,
        TransactionReceiveStorage::class,
    ]
)
@DisplayName("NetxEventSupports")
@TestPropertySource("classpath:application.properties")
class NetxEventSupportsTest(
    private val transactionManager: TransactionManager,
    private val transactionReceiveStorage: TransactionReceiveStorage,
) : StringSpec({

    beforeEach {
        transactionReceiveStorage.clear()
    }

    "event로 객체가 주어지면, TransactionRollbackEvent에서 해당 객체를 decode 할 수 있다." {
        // given
        val expected = Foo("hello", 1.1234567891234568)
        transactionManager.syncStart(expected)

        Thread.sleep(1000)

        // when
        val startEvent = transactionReceiveStorage.pollStart()

        // then
        startEvent.decodeEvent(Foo::class) shouldBeEqualUsingFields expected
    }

    "event로 Map이 주어지면, TransactionRollbackEvent에서 해당 객체를 decode할 수 있다." {
        // given
        val expected = mapOf("name" to "hello")
        val transactionId = transactionManager.syncStart()
        transactionManager.syncJoin(transactionId, expected)

        Thread.sleep(1000)

        // when
        val joinEvent = transactionReceiveStorage.pollJoin()
        val result = joinEvent.decodeEvent(Map::class)

        // then
        result["name"]!! shouldBeEqual expected["name"]!!
    }

    "event로 Int가 주어지면, TransactionRollbackEvent에서 해당 객체를 decode할 수 있다." {
        // given
        val expected = 1
        val transactionId = transactionManager.syncStart()
        transactionManager.syncCommit(transactionId, expected)

        Thread.sleep(1000)

        // when
        val result = transactionReceiveStorage.pollCommit().decodeEvent(Int::class)

        // then
        result shouldBeEqual expected
    }

    "event로 Long이 주어지면, TransactionRollbackEvent에서 해당 객체를 decode할 수 있다." {
        // given
        val expected = 1L
        val transactionId = transactionManager.syncStart()
        transactionManager.syncRollback(transactionId, "cause", expected)

        Thread.sleep(1000)

        // when
        val result = transactionReceiveStorage.pollRollback().decodeEvent(Long::class)

        // then
        result shouldBeEqual expected
    }

    "event로 String이 주어지면, TransactionRollbackEvent에서 해당 객체를 decode할 수 있다." {
        // given
        val expected = "string"
        transactionManager.syncStart(expected)

        Thread.sleep(1000)

        // when
        val result = transactionReceiveStorage.pollStart().decodeEvent(String::class)

        // then
        result shouldBeEqual expected
    }

    "event로 char이 주어지면, TransactionRollbackEvent에서 해당 객체를 decode할 수 있다." {
        // given
        val expected = 'c'
        transactionManager.syncStart(expected)

        Thread.sleep(1000)

        // when
        val result = transactionReceiveStorage.pollStart().decodeEvent(Char::class)

        // then
        result shouldBeEqual expected
    }

    "event로 Boolean이 주어지면, TransactionRollbackEvent에서 해당 객체를 decode할 수 있다." {
        // given
        val expected = true
        transactionManager.syncStart(expected)

        Thread.sleep(1000)

        // when
        val result = transactionReceiveStorage.pollStart().decodeEvent(Boolean::class)

        // then
        result shouldBeEqual expected
    }

    "event로 Unit이 주어지면, TransactionRollbackEvent에서 해당 객체를 decode할 수 있다." {
        // given
        val expected = Unit
        transactionManager.syncStart(expected)

        Thread.sleep(1000)

        // when
        val result = transactionReceiveStorage.pollStart().decodeEvent(Unit::class)

        // then
        result shouldBeEqual expected
    }
}) {

    class Foo(val name: String, val price: Double)
}

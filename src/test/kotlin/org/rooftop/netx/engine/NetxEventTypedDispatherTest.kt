package org.rooftop.netx.engine

import io.kotest.assertions.nondeterministic.eventually
import io.kotest.core.annotation.DisplayName
import io.kotest.core.spec.style.StringSpec
import org.rooftop.netx.api.TransactionManager
import org.rooftop.netx.meta.EnableDistributedTransaction
import org.rooftop.netx.redis.RedisContainer
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.TestPropertySource
import kotlin.time.Duration.Companion.seconds

@EnableDistributedTransaction
@ContextConfiguration(
    classes = [
        RedisContainer::class,
        TransactionTypedReceiveStorage::class,
    ]
)
@DisplayName("NetxEventTypedDispatherTest")
@TestPropertySource("classpath:application.properties")
class NetxEventTypedDispatherTest(
    private val transactionManager: TransactionManager,
    private val transactionTypedReceiveStorage: TransactionTypedReceiveStorage,
) : StringSpec({

    beforeEach {
        transactionTypedReceiveStorage.clear()
    }

    "event로 Foo 타입의 클래스가 주어지면, Any::class, Foo::class의 모든 핸들러에게 트랜잭션이 전파된다." {
        transactionManager.syncStart(Foo("xb"))

        eventually(5.seconds) {
            transactionTypedReceiveStorage.handlerShouldBeEqual(Any::class, 1)
            transactionTypedReceiveStorage.handlerShouldBeEqual(Foo::class, 1)
            transactionTypedReceiveStorage.handlerShouldBeEqual(String::class, 0)
            transactionTypedReceiveStorage.handlerShouldBeEqual(Long::class, 0)
            transactionTypedReceiveStorage.handlerShouldBeEqual(Unit::class, 0)
            transactionTypedReceiveStorage.handlerShouldBeEqual(Boolean::class, 0)
        }
    }

    "event로 String 타입의 클래스가 주어지면, Any::class, String::class의 모든 핸들러에게 트랜잭션이 전파된다." {
        transactionManager.syncStart("String")

        eventually(5.seconds) {
            transactionTypedReceiveStorage.handlerShouldBeEqual(Any::class, 1)
            transactionTypedReceiveStorage.handlerShouldBeEqual(Foo::class, 0)
            transactionTypedReceiveStorage.handlerShouldBeEqual(String::class, 1)
            transactionTypedReceiveStorage.handlerShouldBeEqual(Long::class, 0)
            transactionTypedReceiveStorage.handlerShouldBeEqual(Unit::class, 0)
            transactionTypedReceiveStorage.handlerShouldBeEqual(Boolean::class, 0)
        }
    }

    "event로 Long 타입의 클래스가 주어지면, Any::class, Long::class, String::class, Boolean::class 의 모든 핸들러에게 트랜잭션이 전파된다." {
        transactionManager.syncStart(1000L)

        eventually(5.seconds) {
            transactionTypedReceiveStorage.handlerShouldBeEqual(Any::class, 1)
            transactionTypedReceiveStorage.handlerShouldBeEqual(Foo::class, 0)
            transactionTypedReceiveStorage.handlerShouldBeEqual(String::class, 1)
            transactionTypedReceiveStorage.handlerShouldBeEqual(Long::class, 1)
            transactionTypedReceiveStorage.handlerShouldBeEqual(Unit::class, 0)
            transactionTypedReceiveStorage.handlerShouldBeEqual(Boolean::class, 1)
        }
    }

    "event로 Boolean 타입의 클래스가 주어지면, Any::class, Boolean::class, String::class 의 모든 핸들러에게 트랜잭션이 전파된다." {
        transactionManager.syncStart(true)

        eventually(5.seconds) {
            transactionTypedReceiveStorage.handlerShouldBeEqual(Any::class, 1)
            transactionTypedReceiveStorage.handlerShouldBeEqual(Foo::class, 0)
            transactionTypedReceiveStorage.handlerShouldBeEqual(String::class, 1)
            transactionTypedReceiveStorage.handlerShouldBeEqual(Long::class, 0)
            transactionTypedReceiveStorage.handlerShouldBeEqual(Unit::class, 0)
            transactionTypedReceiveStorage.handlerShouldBeEqual(Boolean::class, 1)
        }
    }

    "event로 어떠한것도 전달되지 않으면, Any::class의 모든 핸들러에게 트랜잭션이 전파된다." {
        transactionManager.syncStart()

        eventually(5.seconds) {
            transactionTypedReceiveStorage.handlerShouldBeEqual(Any::class, 1)
            transactionTypedReceiveStorage.handlerShouldBeEqual(Foo::class, 0)
            transactionTypedReceiveStorage.handlerShouldBeEqual(String::class, 0)
            transactionTypedReceiveStorage.handlerShouldBeEqual(Long::class, 0)
            transactionTypedReceiveStorage.handlerShouldBeEqual(Unit::class, 0)
            transactionTypedReceiveStorage.handlerShouldBeEqual(Boolean::class, 0)
        }
    }

    "event로 Unit이 주어지면, Unit::class의 핸들러에게 트랜잭션이 전파된다." {
        transactionManager.syncStart(Unit)

        eventually(5.seconds) {
            transactionTypedReceiveStorage.handlerShouldBeEqual(Any::class, 1)
            transactionTypedReceiveStorage.handlerShouldBeEqual(Foo::class, 0)
            transactionTypedReceiveStorage.handlerShouldBeEqual(String::class, 0)
            transactionTypedReceiveStorage.handlerShouldBeEqual(Long::class, 0)
            transactionTypedReceiveStorage.handlerShouldBeEqual(Unit::class, 1)
            transactionTypedReceiveStorage.handlerShouldBeEqual(Boolean::class, 0)
        }
    }

}) {

    class Foo(val name: String)
}

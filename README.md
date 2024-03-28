# Netx <img src="https://avatars.githubusercontent.com/u/149151221?s=200&v=4" height = 100 align = left>

> Saga framework / Supports redis-stream and reactive

<img src = "https://github.com/rooftop-MSA/Netx/assets/62425964/08ed9050-1923-42b5-803f-5b7ea37a263f" width="360" align="right"/>

<br>

![version 0.3.7](https://img.shields.io/badge/version-0.3.7-black?labelColor=black&style=flat-square) ![jdk 17](https://img.shields.io/badge/minimum_jdk-17-orange?labelColor=black&style=flat-square) ![load-test](https://img.shields.io/badge/load%20test%2010%2C000%2C000-success-brightgreen?labelColor=black&style=flat-square)    
![redis--stream](https://img.shields.io/badge/-redis--stream-da2020?style=flat-square&logo=Redis&logoColor=white)

Redis-Stream을 지원하는 Saga frame work 입니다.   
`Netx` 는 다음 기능을 제공합니다.

1. 동기 API와 비동기[Reactor](https://projectreactor.io/) API 지원
2. 함수형 Orchestrator 방식과 Event 기반 Choreograph 방식 지원
3. 처리되지 않은 트랜잭션을 찾아 자동으로 재실행
4. Backpressure 지원으로 노드별 처리가능한 트랜잭션 수 조절
5. 여러 노드가 중복 트랜잭션 이벤트를 수신하는 문제 방지
6. `At Least Once` 방식의 메시지 전달 보장

## How to use

Netx는 스프링 환경에서 사용할 수 있으며, 아래와 같이 `@EnableDistributedTransaciton` 어노테이션을 붙이는것으로 손쉽게 사용할 수 있습니다.

```kotlin
@SpringBootApplication
@EnableDistributedTransaciton
class Application {

    companion object {
        @JvmStatic
        fun main(vararg args: String) {
            SpringApplication.run(Application::class.java, *args)
        }
    }
}
```

`@EnableDistributedTransaciton` 어노테이션으로 자동 구성할 경우 netx는 아래 프로퍼티를 사용해 메시지 큐와 커넥션을 맺습니다.

#### Properties

| KEY                     | EXAMPLE   | DESCRIPTION                                                                                                                                                                         | DEFAULT |
|-------------------------|-----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| **netx.mode**           | redis     | 트랜잭션 관리에 사용할 메시지 큐 구현체의 mode 입니다.                                                                                                                                                   |         |
| **netx.host**           | localhost | 트랜잭션 관리에 사용할 메시지 큐 의 host url 입니다. (ex. redis host)                                                                                                                                 |         |
| **netx.password**       | 0000      | 트랜잭션 관리에 사용할 메시지큐에 접속하는데 사용하는 password 입니다. 설정하지 않을시 0000이 비밀번호로 매핑됩니다.                                                                                                             | 0000    |
| **netx.port**           | 6379      | 트랜잭션 관리에 사용할 메시지 큐의 port 입니다.                                                                                                                                                       |         |
| **netx.group**          | pay-group | 분산 노드의 그룹입니다. 트랜잭션 이벤트는 같은 그룹내 하나의 노드로만 전송됩니다.                                                                                                                                      |         |
| **netx.node-id**        | 1         | id 생성에 사용될 식별자입니다. 모든 서버는 반드시 다른 id를 할당받아야 하며, 1~256 만큼의 id를 설정할 수 있습니다. _`중복된 id 생성을 방지하기위해 twitter snowflake 알고리즘으로 id를 생성합니다.`_                                                  |         |
| **netx.node-name**      | pay-1     | _`netx.group`_ 에 참여할 서버의 이름입니다. 같은 그룹내에 중복된 이름이 존재하면 안됩니다.                                                                                                                          |         |
| **netx.recovery-milli** | 1000      | _`netx.recovery-milli`_ 마다 _`netx.orphan-milli`_ 동안 처리 되지 않는 트랜잭션을 찾아 재실행합니다.                                                                                                       | 1000    |
| **netx.orphan-milli**   | 60000     | PENDING 상태가된 트랜잭션 중, orphan-milli가 지나도 ACK 상태가 되지 않은 트랜잭션을 찾아 재시작합니다.                                                                                                               | 60000   |
| **netx.backpressure**   | 40        | 한번에 수신가능한 트랜잭션 수를 조절합니다. **너무 높게설정하면 서버에 부하가 올 수 있고, 낮게 설정하면 성능이 낮아질 수 있습니다.** 이 설정은 다른 서버가 발행한 트랜잭션 수신량과 처리에 실패한 트랜잭션 수신량에 영향을 미칩니다. 수신되지 못하거나, drop된 트랜잭션은 자동으로 retry 대기열에 들어갑니다. | 40      |
| **netx.logging.level**  | info      | logging level을 지정합니다. 선택가능한 value는 다음과 같습니다. "info", "warn", "off"                                                                                                                  | "off"   |
| **netx.pool-size**      | 40        | 커넥션을 계속해서 맺어야할때, 최대 커넥션 수를 조절하는데 사용됩니다.                                                                                                                                             | 10      |

### Usage example

#### Orchestrator-example.

> [!TIP]   
> Orchestrator 사용시, Transactional Message Pattern이 자동 적용됩니다.   
> 메시지 유실에대한 retry 단위는 Orchestrator의 각 연산(하나의 function) 단위이며, 모든 체인이 성공하거나 rollback이 호출됩니다.

```kotlin
// Use Orchestrator
@Service
class OrderService(private val orderOrchestrator: Orchestrator<Order, OrderResponse>) {

    fun order(orderRequest: Order): OrderResult {
        val result = orderOrchestrator.transactionSync(orderRequest)
        if (!result.isSuccess) {
            result.throwError()
        }
        return result.decodeResult(OrderResult::class)
    }
}

// Register Orchestrator
@Configurer
class OrchestratorConfigurer(
    private val orchestratorFactory: OrchestratorFactory
) {

    @Bean
    fun orderOrchestartor(): Orchestrator<Order, OrderResponse> { // <First Request, Last Response>
        return orchestratorFactory.create("orderOrchestrator")
            .start(
                orchestrate = { order -> // its order type
                    // Start Transaction with your bussiness logic 
                    // something like ... "Check valid seller"
                    return@start user
                },
                rollback = { order ->
                    // do rollback logic
                }
            )
            .joinReactive(
                orchestrate = { user -> // Before operations response type "User" flow here 
                    // Webflux supports, should return Mono type.
                },
                // Can skip rollback operation, if you want
            )
            .joinWithContext(
                contextOrchestrate = { context, request ->
                    context.set("key", request) // save data on context
                    context.decode("foo", Foo::class) // The context set in the upstream chain can be retrieved.
                },
            )
            .commit(
                orchestrate = { request ->
                    // When an error occurs, all rollbacks are called from the bottom up, 
                    // starting from the location where the error occurred.
                    throw IllegalArgumentException("Oops! Something went wrong..")
                },
                rollback = { request ->
                    ...
                }
            )
    }
}
```

> [!WARNING]   
> Event사용시 Transactional Message Pattern을 직접 적용해줘야합니다.   
> 아래 이벤트 사용 예시는 적용된 예시가 아니며, 적용을 위해서는 transactionManager 를 호출하는 부분과 트랜잭션 이벤트를 받는부분을 분리해야합니다.   
> 비즈니스로직을 모두 @Transaction...Listener 안으로 이동함으로써 손쉽게 적용할 수 있습니다.

#### Event-example. Start pay transaction

```kotlin
// Sync
fun pay(param: Any): Any {
    val transactionId =
        transactionManager.syncStart(Pay(id = 1L, paid = 1000L)) // start transaction

    runCatching {
        // Do your bussiness logic
    }.fold(
        onSuccess = { transactionManager.syncCommit(transactionId) }, // commit transaction
        onFailure = {
            transactionManager.syncRollback(
                transactionId,
                it.message
            )
        } // rollback transaction
    )
}

// Async
fun pay(param: Any): Mono<Any> {
    return transactionManager.start(
        Pay(
            id = 1L,
            paid = 1000L
        )
    ) // Start distributed transaction and publish transaction start event
        .flatMap { transactionId ->
            service.pay(param)
                .doOnError { throwable ->
                    transactionManager.rollback(
                        transactionId,
                        throwable.message
                    ) // Publish rollback event to all transaction joined node
                }
        }.doOnSuccess { transactionId ->
            transactionManager.commit(transactionId) // Publish commit event to all transaction joined node
        }
}
```

#### Events-Scenario2. Join order transaction

```kotlin
//Sync
fun order(param: Any): Any {
    val transactionId = transactionManager.syncJoin(
        param.transactionId,
        Order(id = 1L, state = PENDING)
    ) // join transaction

    runCatching { // This is kotlin try catch, not netx library spec
        // Do your bussiness logic
    }.fold(
        onSuccess = { transactionManager.syncCommit(transactionId) }, // commit transaction
        onFailure = {
            transactionManager.syncRollback(
                transactionId,
                it.message
            )
        } // rollback transaction
    )
}

// Async
fun order(param: Any): Mono<Any> {
    return transactionManager.join(
        param.transactionId,
        Order(id = 1L, state = PENDING)
    ) // join exists distributed transaction and publish transaction join event
        .flatMap { transactionId ->
            service.order(param)
                .doOnError { throwable ->
                    transactionManager.rollback(transactionId, throwable.message)
                }
        }.doOnSuccess { transactionId ->
            transactionManager.commit(transactionId)
        }
}
```

#### Events-Scenario3. Check exists transaction

```kotlin
// Sync
fun exists(param: Any): Any {
    return transactionManager.syncExists(param.transactionId)
}

// Async
fun exists(param: Any): Mono<Any> {
    return transactionManager.exists(param.transactionId) // Find any transaction has ever been started 
}
```

#### Events-Scenario4. Handle transaction event

다른 분산서버가 (혹은 자기자신이) transactionManager를 통해서 트랜잭션을 시작하거나 트랜잭션 상태를 변경했을때, 트랜잭션 상태에 맞는 핸들러를 호출합니다.
이 핸들러를 구현함으로써, 트랜잭션 상태별 로직을 구현할 수 있습니다.
각 핸들러에서 에러가 던져지면, 자동으로 rollback 이 호출됩니다.

> [!WARNING]   
> 트랜잭션 핸들러는 반드시 핸들러에 맞는 `TransactionEvent` **하나**만을 파라미터로 받아야 합니다.

```kotlin

@TransactionHandler
class TransactionHandler {

    @TransactionStartListener(event = Foo::class) // Receive transaction event when event can be mapped to Foo.class
    fun handleTransactionStartEvent(event: TransactionStartEvent) {
        val foo: Foo = event.decodeEvent(Foo::class) // Get event field to Foo.class
        // ...
        event.setNextEvent(nextFoo) // When this handler terminates and calls the next event or rollback, the event set here is published together.
    }

    @TransactionJoinHandler(successWith = SuccessWith.PUBLISH_COMMIT) // Receive all transaction event when no type is defined. And, when terminated this function, publish commit state
    fun handleTransactionJoinEvent(event: TransactionJoinEvent) {
        // ...
    }

    @TransactionCommitHandler(
        event = Foo::class,
        noRollbackFor = [IllegalArgumentException::class] // Dont rollback when throw IllegalArgumentException. *Rollback if throw Throwable or IllegalArgumentException's super type* 
    )
    fun handleTransactionCommitEvent(event: TransactionCommitEvent): Mono<String> { // In Webflux framework, publisher must be returned.
        throw IllegalArgumentException("Ignore this exception")
        // ...
    }

    @TransactionRollbackHandler(Foo::class)
    fun handleTransactionRollbackEvent(event: TransactionRollbackEvent) { // In Mvc framework, publisher must not returned.
        val undo: Foo = event.decodeUndo(Foo::class) // Get event field to Foo.class
    }
}
```

## Download

```groovy
repositories {
    maven { url "https://jitpack.io" }
}

dependencies {
    implementation "com.github.rooftop-msa:netx:${version}"
}
```

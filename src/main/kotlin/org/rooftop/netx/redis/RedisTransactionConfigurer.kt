package org.rooftop.netx.redis

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule
import org.rooftop.netx.api.TransactionManager
import org.rooftop.netx.engine.*
import org.rooftop.netx.engine.core.Transaction
import org.rooftop.netx.engine.logging.LoggerFactory
import org.rooftop.netx.engine.logging.info
import org.rooftop.netx.engine.logging.logger
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory
import org.springframework.data.redis.connection.RedisPassword
import org.springframework.data.redis.connection.RedisStandaloneConfiguration
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer
import org.springframework.data.redis.serializer.RedisSerializationContext
import org.springframework.data.redis.serializer.StringRedisSerializer


@Configuration
internal class RedisTransactionConfigurer(
    @Value("\${netx.host}") private val host: String,
    @Value("\${netx.port}") private val port: String,
    @Value("\${netx.password:0000}") private val password: String,
    @Value("\${netx.group}") private val nodeGroup: String,
    @Value("\${netx.node-id}") private val nodeId: Int,
    @Value("\${netx.node-name}") private val nodeName: String,
    @Value("\${netx.recovery-milli:1000}") private val recoveryMilli: Long,
    @Value("\${netx.orphan-milli:60000}") private val orphanMilli: Long,
    @Value("\${netx.backpressure:40}") private val backpressureSize: Int,
    @Value("\${netx.logging.level:off}") loggingLevel: String,
    @Value("\${netx.pool-size:10}") private val poolSize: Int,
    private val applicationContext: ApplicationContext,
) {

    init {
        logger = LoggerFactory.getLogger(loggingLevel)
    }

    @Bean
    @ConditionalOnProperty(prefix = "netx", name = ["mode"], havingValue = "redis")
    fun redisStreamOrchestratorFactory(): OrchestratorFactory = OrchestratorFactory(
        transactionManager = redisStreamTransactionManager(),
        transactionDispatcher = redisStreamTransactionDispatcher(),
        codec = jsonCodec(),
        resultHolder = redisResultHolder(),
        requestHolder = redisRequestHolder(),
    )

    @Bean
    @ConditionalOnProperty(prefix = "netx", name = ["mode"], havingValue = "redis")
    fun redisStreamTransactionManager(): TransactionManager =
        RedisStreamTransactionManager(
            nodeName = nodeName,
            nodeGroup = nodeGroup,
            reactiveRedisTemplate = transactionReactiveRedisTemplate(),
            codec = jsonCodec(),
            transactionIdGenerator = tsidTransactionIdGenerator(),
            objectMapper = netxObjectMapper(),
        ).also {
            info("RedisStreamTransactionManager connect to host : \"$host\" port : \"$port\" nodeName : \"$nodeName\" nodeGroup : \"$nodeGroup\"")
        }

    @Bean
    @ConditionalOnProperty(prefix = "netx", name = ["mode"], havingValue = "redis")
    fun tsidTransactionIdGenerator(): TransactionIdGenerator = TransactionIdGenerator(nodeId)

    @Bean
    @ConditionalOnProperty(prefix = "netx", name = ["mode"], havingValue = "redis")
    fun redisStreamTransactionListener(): RedisStreamTransactionListener =
        RedisStreamTransactionListener(
            backpressureSize = backpressureSize,
            transactionDispatcher = redisStreamTransactionDispatcher(),
            connectionFactory = reactiveRedisConnectionFactory(),
            nodeGroup = nodeGroup,
            nodeName = nodeName,
            reactiveRedisTemplate = transactionReactiveRedisTemplate(),
            objectMapper = netxObjectMapper(),
        ).also {
            info("RedisStreamTransactionListener connect to host : \"$host\" port : \"$port\" nodeName : \"$nodeName\" nodeGroup : \"$nodeGroup\" backpressureSize : \"$backpressureSize\"")
            it.subscribeStream()
        }

    @Bean
    @ConditionalOnProperty(prefix = "netx", name = ["mode"], havingValue = "redis")
    fun redisResultHolder(): ResultHolder =
        RedisResultHolder(
            poolSize,
            jsonCodec(),
            netxObjectMapper(),
            reactiveRedisTemplate(),
        )

    @Bean
    @ConditionalOnProperty(prefix = "netx", name = ["mode"], havingValue = "redis")
    fun redisRequestHolder(): RequestHolder =
        RedisRequestHolder(
            jsonCodec(),
            reactiveRedisTemplate(),
        )

    @Bean
    @ConditionalOnProperty(prefix = "netx", name = ["mode"], havingValue = "redis")
    fun jsonCodec(): JsonCodec = JsonCodec(netxObjectMapper())

    @Bean
    @ConditionalOnProperty(prefix = "netx", name = ["mode"], havingValue = "redis")
    fun netxObjectMapper(): ObjectMapper =
        ObjectMapper().registerModule(ParameterNamesModule(JsonCreator.Mode.PROPERTIES))
            .registerModule(KotlinModule.Builder().build())
            .registerModule(JavaTimeModule())

    @Bean
    @ConditionalOnProperty(prefix = "netx", name = ["mode"], havingValue = "redis")
    fun redisTransactionRetrySupporter(): RedisTransactionRetrySupporter =
        RedisTransactionRetrySupporter(
            nodeGroup = nodeGroup,
            nodeName = nodeName,
            reactiveRedisTemplate = transactionReactiveRedisTemplate(),
            transactionDispatcher = redisStreamTransactionDispatcher(),
            orphanMilli = orphanMilli,
            recoveryMilli = recoveryMilli,
            backpressureSize = backpressureSize,
            objectMapper = netxObjectMapper(),
        ).also {
            info("RedisTransactionRetrySupporter connect to host : \"$host\" port : \"$port\" nodeName : \"$nodeName\" nodeGroup : \"$nodeGroup\" orphanMilli : \"$orphanMilli\" recoveryMilli : \"$recoveryMilli\" backpressureSize : \"$backpressureSize\"")
            it.watchOrphanTransaction()
        }

    @Bean
    @ConditionalOnProperty(prefix = "netx", name = ["mode"], havingValue = "redis")
    internal fun redisStreamTransactionDispatcher(): RedisStreamTransactionDispatcher =
        RedisStreamTransactionDispatcher(
            applicationContext = applicationContext,
            reactiveRedisTemplate = transactionReactiveRedisTemplate(),
            nodeGroup = nodeGroup,
            codec = jsonCodec(),
            transactionManager = redisStreamTransactionManager(),
        ).also {
            info("RedisStreamTransactionDispatcher connect to host : \"$host\" port : \"$port\" nodeName : \"$nodeName\" nodeGroup : \"$nodeGroup\"")
        }

    @Bean
    @ConditionalOnProperty(prefix = "netx", name = ["mode"], havingValue = "redis")
    internal fun transactionReactiveRedisTemplate(): ReactiveRedisTemplate<String, Transaction> {
        val keySerializer = StringRedisSerializer()
        val valueSerializer =
            Jackson2JsonRedisSerializer(netxObjectMapper(), Transaction::class.java)

        val builder: RedisSerializationContext.RedisSerializationContextBuilder<String, Transaction> =
            RedisSerializationContext.newSerializationContext(keySerializer)

        val context = builder.value(valueSerializer).build()

        return ReactiveRedisTemplate(reactiveRedisConnectionFactory(), context);
    }

    @Bean
    @ConditionalOnProperty(prefix = "netx", name = ["mode"], havingValue = "redis")
    fun reactiveRedisTemplate(): ReactiveRedisTemplate<String, String> {
        val keySerializer = StringRedisSerializer()
        val valueSerializer = StringRedisSerializer()

        val builder: RedisSerializationContext.RedisSerializationContextBuilder<String, String> =
            RedisSerializationContext.newSerializationContext(keySerializer)

        val context = builder.value(valueSerializer).build()

        return ReactiveRedisTemplate(reactiveRedisConnectionFactory(), context);
    }

    @Bean
    @ConditionalOnProperty(prefix = "netx", name = ["mode"], havingValue = "redis")
    fun reactiveRedisConnectionFactory(): ReactiveRedisConnectionFactory {
        val port: String = System.getProperty("netx.port") ?: port

        val redisStandaloneConfiguration = RedisStandaloneConfiguration()
        redisStandaloneConfiguration.hostName = host
        redisStandaloneConfiguration.port = port.toInt()
        redisStandaloneConfiguration.password = RedisPassword.of(password)

        return LettuceConnectionFactory(redisStandaloneConfiguration)
    }
}

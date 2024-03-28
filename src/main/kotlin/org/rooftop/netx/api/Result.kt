package org.rooftop.netx.api

import kotlin.reflect.KClass

class Result<T : Any> private constructor(
    val isSuccess: Boolean,
    private val codec: Codec,
    private val result: String?,
    private val error: Error? = null,
) {

    fun decodeResultOrThrow(typeReference: TypeReference<T>): T = decodeResult(typeReference)

    fun decodeResultOrThrow(type: Class<T>): T = decodeResultOrThrow(type.kotlin)

    fun decodeResultOrThrow(type: KClass<T>): T {
        if (!isSuccess) {
            throwError()
        }
        return decodeResult(type)
    }

    fun decodeResult(typeReference: TypeReference<T>): T = result?.let {
        codec.decode(it, typeReference)
    } ?: throw ResultException("Cannot decode result cause Result is fail state")

    fun decodeResult(type: Class<T>): T = decodeResult(type.kotlin)

    fun decodeResult(type: KClass<T>): T = result?.let {
        codec.decode(it, type)
    } ?: throw ResultException("Cannot decode result cause Result is fail state")

    fun throwError() = error?.throwError(codec)
        ?: throw ResultException("Cannot throw error cause Result is success state")

    override fun toString(): String {
        return "Result(isSuccess=$isSuccess, codec=$codec, result=$result, error=$error)"
    }

    private class Error(
        private val error: String,
        private val type: KClass<Throwable>
    ) {

        fun throwError(codec: Codec) {
            throw codec.decode(error, type)
        }

        override fun toString(): String {
            return "Error(error='$error', type=$type)"
        }
    }

    internal companion object {

        fun <T : Any> success(
            codec: Codec,
            result: String,
        ): Result<T> {
            return Result(
                true,
                codec,
                result,
                null,
            )
        }

        fun <T : Any> fail(
            codec: Codec,
            error: String,
            type: KClass<Throwable>,
        ): Result<T> {
            return Result(
                false,
                codec,
                null,
                Error(error, type),
            )
        }
    }
}

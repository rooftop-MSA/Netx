package org.rooftop.netx.client

import org.springframework.boot.test.context.TestComponent
import reactor.core.publisher.Flux

@TestComponent
class LoadRunner {

    fun load(count: Int, behavior: Runnable) {
        val iter = mutableListOf<Runnable>()
        for (i in 1..count) {
            iter.add(behavior)
        }
        Flux.fromIterable(iter)
            .parallel(200)
            .map { it.run() }
            .subscribe()
    }
}

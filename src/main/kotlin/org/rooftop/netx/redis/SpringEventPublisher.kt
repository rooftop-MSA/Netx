package org.rooftop.netx.redis

import org.rooftop.netx.engine.EventPublisher
import org.springframework.context.ApplicationEventPublisher

class SpringEventPublisher(private val eventPublisher: ApplicationEventPublisher) : EventPublisher {

    override fun publish(event: Any) {
        eventPublisher.publishEvent(event)
    }
}

package de.digitalfrontiers.axon.statemachine.domain

import org.axonframework.eventhandling.EventBus
import org.axonframework.eventhandling.EventMessage
import org.axonframework.eventhandling.GenericEventMessage
import org.axonframework.extensions.reactor.eventhandling.gateway.ReactorEventGateway
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

fun <T> EventBus.publishEvent(event: T) =
    GenericEventMessage
        .asEventMessage<T>(event)
        .let { publish(it) }

fun <T> ReactorEventGateway.publishEvent(event: T): Flux<T> =
    GenericEventMessage
        .asEventMessage<T>(event)
        .let { publish(it) }
        .map { it.payload as T }

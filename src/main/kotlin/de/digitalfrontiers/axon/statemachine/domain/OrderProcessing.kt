package de.digitalfrontiers.axon.statemachine.domain

import com.fasterxml.jackson.annotation.JsonIgnore
import org.axonframework.config.ProcessingGroup
import org.axonframework.extensions.reactor.commandhandling.gateway.ReactorCommandGateway
import org.axonframework.extensions.reactor.eventhandling.gateway.ReactorEventGateway
import org.axonframework.modelling.saga.SagaEventHandler
import org.axonframework.modelling.saga.SagaLifecycle
import org.axonframework.modelling.saga.StartSaga
import org.axonframework.spring.stereotype.Saga
import org.springframework.beans.factory.annotation.Autowired
import java.util.*

@Saga
@ProcessingGroup("order")
class OrderProcessing {

    @JsonIgnore
    @Autowired
    lateinit var commandGateway: ReactorCommandGateway

    @JsonIgnore
    @Autowired
    lateinit var eventGateway: ReactorEventGateway

    var orderId: UUID? = null
    var productItems: Map<UUID, Long>? = null
    var totalPrice: Double? = null

    var paid = false
    var invoiceId: UUID? = null
    var delivered = false
    var shipmentId: UUID? = null

    @StartSaga
    @SagaEventHandler(associationProperty = "orderId")
    fun on(event: OrderPlacedEvent) {
        orderId = event.orderId
        productItems = event.productItems
        totalPrice = event.totalPrice

        commandGateway
            .send<Boolean>(ReserveCreditCommand(amount = event.totalPrice))
            .flatMapMany { when (it) {
                true -> {
                    eventGateway.publishEvent(
                        CreditReservedEvent(
                            orderId = event.orderId,
                            amount = event.totalPrice
                        )
                    )
                }
                false -> {
                    commandGateway.send<Unit>(
                        DenyOrderCommand(
                            orderId = event.orderId,
                            reason = "insufficient credits"
                        )
                    ).map { SagaLifecycle.end() }
                }
            }}.subscribe()
    }

    @SagaEventHandler(associationProperty = "orderId")
    fun on(event: CreditReservedEvent) {
        commandGateway
            .send<UUID>(
                SendInvoiceCommand(
                    orderId = event.orderId,
                    productItems = requireNotNull(productItems),
                    totalPrice = requireNotNull(totalPrice)
                )
            )
            .flatMapMany {
                SagaLifecycle.associateWith("invoiceId", invoiceId.toString())
                eventGateway.publishEvent(
                    InvoiceRequestedEvent(invoiceId = it)
                )
            }.subscribe()
    }

    @SagaEventHandler(associationProperty = "invoiceId")
    fun on(event: InvoiceRequestedEvent) {
        commandGateway
            .send<UUID>(
                ShipItemsCommand(productItems = requireNotNull(productItems))
            )
            .flatMapMany {
                SagaLifecycle.associateWith("shipmentId", shipmentId.toString())
                eventGateway.publishEvent(
                    ShipmentRequestedEvent(shipmentId = it)
                )
            }.subscribe()
    }

    @SagaEventHandler(associationProperty = "invoiceId")
    fun on(event: InvoicePaidEvent) {
        invoiceId = event.invoiceId
        paid = true

        finishIfNecessary()
    }

    @SagaEventHandler(associationProperty = "shipmentId")
    fun on(event: ShipmentDeliveredEvent) {
        shipmentId = event.shipmentId
        delivered = true

        finishIfNecessary()
    }

    private fun finishIfNecessary() {
        if (paid && delivered) {
            commandGateway
                .send<Unit>(
                    MarkOrderCompleteCommand(
                        orderId = requireNotNull(orderId),
                        invoiceId = requireNotNull(invoiceId),
                        shipmentId = requireNotNull(shipmentId)
                    )
                )
                .map { SagaLifecycle.end() }
                .subscribe()
        }
    }
}

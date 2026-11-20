package com.siose.ff.logistics.transportation.messaging;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.siose.ff.logistics.transportation.ShipmentDeliveries;
import com.siose.ff.logistics.transportation.ShipmentDeliveries.Outcome;
import com.siose.ff.logistics.transportation.StopDelivery;
import java.time.Instant;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Flow 3 on the services side: a completed route stop delivers the shipment it carried. The
 * modulith does the same with an in-process listener ({@code flows.StopsToShipments}).
 *
 * <p>The message is routing's {@code RouteStopCompleted} as JSON. This service does not have
 * routing's classes: it reads the fields it needs into a type of its own and ignores the rest, so
 * routing may add fields without breaking it. The topic's name and the fields are the contract.
 * Spring Modulith's Kafka support turns the JSON into that type before the method is called (its
 * message converter, on bytes), where warehousing reads a String and parses it itself.
 *
 * <ul>
 *   <li><b>At least once.</b> The same message may be read twice; delivering is idempotent
 *       ({@link ShipmentDeliveries}), so a repeat changes nothing.</li>
 *   <li><b>A chain.</b> The delivery publishes {@code ShipmentDelivered} through this service's
 *       outbox, as one made through the API does: a shipment to a warehouse opens an arrival there
 *       (flow 2).</li>
 *   <li><b>A message that cannot be read</b> goes to this consumer's dead-letter topic at once; one
 *       that fails otherwise is tried again a few times first ({@link KafkaErrorHandling}).</li>
 * </ul>
 */
@Component
class CompletedStops {

    /** Routing's topic (its {@code RouteStopCompleted.TOPIC}). */
    static final String TOPIC = "ff.logistics.geo.route-stop-completed";

    private static final Logger log = LoggerFactory.getLogger(CompletedStops.class);

    /** The fields of routing's event that this service reads; the others are ignored. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    record RouteStopCompletedMessage(UUID routePlanId, String tenantId, String routeNumber, String stopCode,
            String shipmentNumber, String receivedBy, Instant completedAt) {
    }

    private final ShipmentDeliveries deliveries;

    CompletedStops(ShipmentDeliveries deliveries) {
        this.deliveries = deliveries;
    }

    @KafkaListener(topics = TOPIC)
    void on(RouteStopCompletedMessage completed) {
        if (completed.tenantId() == null || completed.receivedBy() == null || completed.completedAt() == null) {
            throw new IllegalArgumentException("A completed stop without tenant, signature or time: " + completed);
        }
        if (completed.shipmentNumber() == null) {
            return;
        }
        Outcome outcome = deliveries.deliver(new StopDelivery(completed.tenantId(), completed.shipmentNumber(),
                completed.receivedBy(), completed.completedAt(), completed.routeNumber(), completed.stopCode()));
        log.debug("Stop {} of {} read from Kafka: {}", completed.stopCode(), completed.routeNumber(), outcome);
    }
}

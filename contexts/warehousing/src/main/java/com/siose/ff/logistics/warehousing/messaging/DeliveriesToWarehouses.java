package com.siose.ff.logistics.warehousing.messaging;

import com.siose.ff.logistics.warehousing.InboundArrivals;
import com.siose.ff.logistics.warehousing.InboundArrivals.Outcome;
import com.siose.ff.logistics.warehousing.ShipmentArrival;
import java.time.Instant;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;

/**
 * Flow 2 on the services side: a shipment delivered to a warehouse opens an inbound arrival there.
 * The modulith does the same with an in-process listener ({@code flows.DeliveriesToWarehouses}).
 *
 * <p>The message is transportation's {@code ShipmentDelivered} as JSON. This service does not have
 * transportation's classes: it reads the fields it needs into a type of its own, and ignores the
 * rest, so transportation may add fields without breaking it. The topic's name and the fields are
 * the contract between the two services.
 *
 * <ul>
 *   <li><b>At least once.</b> Kafka may hand the same message over again (a rebalance, a crash
 *       before the offset was committed), and the outbox may send it twice. Opening an arrival is
 *       idempotent ({@link InboundArrivals}), so a repeat changes nothing.</li>
 *   <li><b>Order.</b> There is one delivery per shipment, so there is nothing to order.</li>
 *   <li><b>A message that cannot be read</b> goes to this consumer's dead-letter topic at once; one
 *       that fails otherwise (the database away) is tried again a few times first
 *       ({@link KafkaErrorHandling}).</li>
 * </ul>
 */
@Component
class DeliveriesToWarehouses {

    /** Transportation's topic (its {@code ShipmentDelivered.TOPIC}). */
    static final String TOPIC = "ff.logistics.tms.shipment-delivered";

    private static final Logger log = LoggerFactory.getLogger(DeliveriesToWarehouses.class);

    private static final JsonMapper JSON = JsonMapper.builder()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    /** The fields of transportation's event that this service reads. */
    record ShipmentDeliveredMessage(UUID shipmentId, String tenantId, String shipmentNumber, String destinationGln,
            String sscc, double grossWeightKg, Instant deliveredAt) {
    }

    private final InboundArrivals arrivals;

    DeliveriesToWarehouses(InboundArrivals arrivals) {
        this.arrivals = arrivals;
    }

    @KafkaListener(topics = TOPIC)
    void on(String message) {
        ShipmentDeliveredMessage delivered = JSON.readValue(message, ShipmentDeliveredMessage.class);
        if (delivered.shipmentId() == null || delivered.tenantId() == null) {
            throw new IllegalArgumentException("A delivery without shipment or tenant: " + message);
        }
        if (delivered.destinationGln() == null) {
            return;
        }
        Outcome outcome = arrivals.open(new ShipmentArrival(delivered.tenantId(), delivered.shipmentId(),
                delivered.shipmentNumber(), delivered.destinationGln(), delivered.sscc(), delivered.grossWeightKg(),
                delivered.deliveredAt()));
        log.debug("Delivery of {} read from Kafka: {}", delivered.shipmentNumber(), outcome);
    }
}

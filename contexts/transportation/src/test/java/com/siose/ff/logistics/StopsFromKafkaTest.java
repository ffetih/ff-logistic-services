package com.siose.ff.logistics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.jayway.jsonpath.JsonPath;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.RequestPostProcessor;

/**
 * Flow 2's second half on the services side: a delivery read from Kafka opens an inbound arrival;
 * the same message twice opens one; a message that cannot be read goes to the dead-letter topic and
 * the deliveries behind it still arrive. The messages are written as transportation writes them.
 */
@SpringBootTest(properties = {
        "spring.kafka.listener.auto-startup=true",
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.datasource.url=jdbc:h2:mem:transportation_stops;DB_CLOSE_DELAY=-1;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE"
})
@AutoConfigureMockMvc
@EmbeddedKafka(topics = {StopsFromKafkaTest.TOPIC, StopsFromKafkaTest.TOPIC + ".transportation.dlt"})
@DirtiesContext
class StopsFromKafkaTest {

    static final String TOPIC = "ff.logistics.geo.route-stop-completed";
    private static final String TENANT = "t-kafka-stops";

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private EmbeddedKafkaBroker broker;

    /**
     * Routing's side, as a plain String producer. Not the service's own template: Spring Modulith
     * sets that one to send bytes, for its events.
     */
    private KafkaTemplate<String, String> kafka;

    @BeforeEach
    void routingSends() {
        kafka = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(KafkaTestUtils.producerProps(broker),
                new StringSerializer(), new StringSerializer()));
    }

    private static RequestPostProcessor planner() {
        return jwt().jwt(token -> token.subject("planner1").claim("tenant_id", TENANT))
                .authorities(new SimpleGrantedAuthority("ROLE_LOGISTICS_PLANNER"));
    }

    /** A shipment on its way: its id and number. */
    private String[] aShipment() throws Exception {
        String body = mockMvc.perform(post("/api/v1/tms/shipments").contentType(MediaType.APPLICATION_JSON).content("""
                        {"consignorName": "FF Hadimkoy", "consigneeName": "Esenyurt Depot Shop",
                         "equipmentCode": "CSQU3054383", "sscc": "106141411234567897", "incoterm": "DAP",
                         "origin": "Hadimkoy Hub", "destination": "Esenyurt", "grossWeightKg": 800,
                         "legs": [{"mode": "ROAD", "origin": "Hadimkoy Hub", "destination": "Esenyurt",
                                   "distanceKm": 9.87, "carrierReference": "TR-34-FF-0417"}]}
                        """).with(planner()))
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString();
        return new String[] {JsonPath.read(body, "$.id"), JsonPath.read(body, "$.shipmentNumber")};
    }

    /** Routing's RouteStopCompleted, as its outbox writes it, with a field this service does not read. */
    private static String completed(UUID route, String shipmentNumber) {
        return """
                {"routePlanId": "%s", "tenantId": "%s", "routeNumber": "RTE-5003", "stopCode": "IST-ESN",
                 "shipmentNumber": "%s", "receivedBy": "Ayse Demir", "completedAt": "2026-11-21T09:15:00Z",
                 "driverNote": "left at the back door"}
                """.formatted(route, TENANT, shipmentNumber);
    }

    private String statusOf(String shipmentId) throws Exception {
        String body = mockMvc.perform(get("/api/v1/tms/shipments/" + shipmentId).with(planner()))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
        return JsonPath.read(body, "$.status");
    }

    @Test
    void aStopReadTwiceDeliversItsShipmentOnce() throws Exception {
        String[] shipment = aShipment();
        UUID route = UUID.randomUUID();
        kafka.send(TOPIC, route.toString(), completed(route, shipment[1]));
        kafka.send(TOPIC, route.toString(), completed(route, shipment[1]));

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(statusOf(shipment[0])).isEqualTo("DELIVERED"));
        String body = mockMvc.perform(get("/api/v1/tms/shipments/" + shipment[0]).with(planner()))
                .andReturn().getResponse().getContentAsString();
        assertThat((String) JsonPath.read(body, "$.receivedBy")).isEqualTo("Ayse Demir");
        assertThat((String) JsonPath.read(body, "$.deliveredAt")).isEqualTo("2026-11-21T09:15:00Z");
        // The second message has been read too (the next one behind it is handled), and changed nothing.
        String[] next = aShipment();
        kafka.send(TOPIC, route.toString(), completed(route, next[1]));
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(statusOf(next[0])).isEqualTo("DELIVERED"));
    }

    @Test
    void aMessageThatCannotBeReadGoesToTheDeadLetterTopicAndTheRestGoOn() throws Exception {
        String[] after = aShipment();
        UUID route = UUID.randomUUID();
        kafka.send(TOPIC, "broken", "this is not JSON");
        kafka.send(TOPIC, route.toString(), completed(route, after[1]));

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(statusOf(after[0])).isEqualTo("DELIVERED"));

        Map<String, Object> props = KafkaTestUtils.consumerProps(broker, "test-dlt-reader", false);
        props.put("auto.offset.reset", "earliest");
        try (Consumer<String, String> consumer = new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(),
                new StringDeserializer()).createConsumer()) {
            consumer.subscribe(List.of(TOPIC + ".transportation.dlt"));
            ConsumerRecord<String, String> dead = KafkaTestUtils.getSingleRecord(consumer, TOPIC + ".transportation.dlt",
                    Duration.ofSeconds(15));
            assertThat(dead.key()).isEqualTo("broken");
            assertThat(dead.value()).isEqualTo("this is not JSON");
            assertThat(dead.headers().lastHeader("kafka_dlt-exception-message")).isNotNull();
        }
    }
}

package com.siose.ff.logistics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.jayway.jsonpath.JsonPath;
import com.siose.ff.logistics.standards.Gs1Standards;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
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
        "spring.datasource.url=jdbc:h2:mem:warehousing_kafka;DB_CLOSE_DELAY=-1;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE"
})
@AutoConfigureMockMvc
@EmbeddedKafka(topics = {DeliveriesFromKafkaTest.TOPIC, DeliveriesFromKafkaTest.TOPIC + ".warehousing.dlt"})
@DirtiesContext
class DeliveriesFromKafkaTest {

    static final String TOPIC = "ff.logistics.tms.shipment-delivered";
    private static final String TENANT = "t-kafka-wh";
    private static final String PREFIX = "8690004";
    private static final String GLN = PREFIX + "00001" + Gs1Standards.checkDigit(PREFIX + "00001");

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private KafkaTemplate<String, String> kafka;

    @Autowired
    private EmbeddedKafkaBroker broker;

    private static RequestPostProcessor operator() {
        return jwt().jwt(token -> token.subject("operator1").claim("tenant_id", TENANT))
                .authorities(new SimpleGrantedAuthority("ROLE_WAREHOUSE_OPERATOR"));
    }

    @BeforeEach
    void aWarehouse() throws Exception {
        mockMvc.perform(get("/api/v1/wms/warehouses").with(operator())).andExpect(status().isOk());
        String existing = mockMvc.perform(get("/api/v1/wms/warehouses").with(operator()))
                .andReturn().getResponse().getContentAsString();
        if (((List<?>) JsonPath.read(existing, "$")).isEmpty()) {
            mockMvc.perform(post("/api/v1/wms/warehouses").contentType(MediaType.APPLICATION_JSON).content("""
                            {"code": "CGN-KAFKA", "name": "Cologne Eifeltor Hub", "gln": "%s", "gs1CompanyPrefix": "%s",
                             "countryCode": "DE", "city": "Cologne", "latitude": 50.9, "longitude": 6.9,
                             "palletCapacity": 500}
                            """.formatted(GLN, PREFIX)).with(operator()))
                    .andExpect(status().isCreated());
        }
    }

    /** Transportation's ShipmentDelivered, as its outbox writes it, with a field this service does not read. */
    private static String delivered(UUID shipment) {
        return """
                {"shipmentId": "%s", "tenantId": "%s", "shipmentNumber": "SHP-20001", "customerOrderReference": null,
                 "totalDistanceKm": 2600.0, "savedCo2eKg": 0.0, "deliveredAt": "2026-11-20T10:00:00Z",
                 "destinationGln": "%s", "sscc": "106141411234567897", "grossWeightKg": 22000.0}
                """.formatted(shipment, TENANT, GLN);
    }

    private List<String> arrivalsFor(UUID shipment) throws Exception {
        String body = mockMvc.perform(get("/api/v1/wms/arrivals").with(operator()))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
        List<String> ids = JsonPath.read(body, "$[*].shipmentId");
        return ids.stream().filter(shipment.toString()::equals).toList();
    }

    @Test
    void aDeliveryReadTwiceOpensOneArrival() throws Exception {
        UUID shipment = UUID.randomUUID();
        kafka.send(TOPIC, shipment.toString(), delivered(shipment));
        kafka.send(TOPIC, shipment.toString(), delivered(shipment));

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(arrivalsFor(shipment)).hasSize(1));
        // The second message has been read too (the next one behind it arrives), and changed nothing.
        UUID next = UUID.randomUUID();
        kafka.send(TOPIC, next.toString(), delivered(next));
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(arrivalsFor(next)).hasSize(1));
        assertThat(arrivalsFor(shipment)).hasSize(1);
    }

    @Test
    void aMessageThatCannotBeReadGoesToTheDeadLetterTopicAndTheRestGoOn() throws Exception {
        UUID after = UUID.randomUUID();
        kafka.send(TOPIC, "broken", "this is not JSON");
        kafka.send(TOPIC, after.toString(), delivered(after));

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(arrivalsFor(after)).hasSize(1));

        Map<String, Object> props = KafkaTestUtils.consumerProps(broker, "test-dlt-reader", false);
        props.put("auto.offset.reset", "earliest");
        try (Consumer<String, String> consumer = new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(),
                new StringDeserializer()).createConsumer()) {
            consumer.subscribe(List.of(TOPIC + ".warehousing.dlt"));
            ConsumerRecord<String, String> dead = KafkaTestUtils.getSingleRecord(consumer, TOPIC + ".warehousing.dlt",
                    Duration.ofSeconds(15));
            assertThat(dead.key()).isEqualTo("broken");
            assertThat(dead.value()).isEqualTo("this is not JSON");
            assertThat(dead.headers().lastHeader("kafka_dlt-exception-message")).isNotNull();
        }
    }
}

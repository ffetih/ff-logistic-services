package com.siose.ff.logistics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.jayway.jsonpath.JsonPath;
import com.siose.ff.contracts.EventContract;
import com.siose.ff.logistics.transportation.ShipmentDelivered;
import com.siose.ff.logistics.transportation.ShipmentProgressed;
import java.time.Duration;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.RequestPostProcessor;

/**
 * Flow 2's first half on the services side: a delivery leaves this service through its outbox. The
 * registry stores the event with the delivery, in the schema {@code transportation}, and sends it to
 * Kafka after the commit, keyed by the shipment, as JSON with what the warehouse needs. The broker
 * runs inside the test JVM, so what a consumer reads is checked for real.
 *
 * <p>Every message read back is checked against its contract (ff-contracts, S06-03): the modulith's
 * test runs the same check, so a reader cannot tell which side sent a message.
 */
@SpringBootTest(properties = {
        "spring.modulith.events.externalization.enabled=true",
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.admin.auto-create=true",
        // A database of its own: this context's outbox rows must not mix with the other contexts'.
        "spring.datasource.url=jdbc:h2:mem:transportation_kafka;DB_CLOSE_DELAY=-1;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE"
})
@AutoConfigureMockMvc
@EmbeddedKafka
@DirtiesContext
class ShipmentEventsToKafkaTest {

    private static final String GLN = "8690001000011";

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private EmbeddedKafkaBroker broker;

    @Autowired
    private JdbcTemplate jdbc;

    private static RequestPostProcessor planner() {
        return jwt().jwt(token -> token.subject("planner1").claim("tenant_id", "t-kafka"))
                .authorities(new SimpleGrantedAuthority("ROLE_LOGISTICS_PLANNER"));
    }

    @Test
    void aDeliveryReachesItsTopicKeyedByTheShipmentWithTheWarehouse() throws Exception {
        String body = mockMvc.perform(post("/api/v1/tms/shipments").contentType(MediaType.APPLICATION_JSON).content("""
                        {"consignorName": "Bosch Bursa", "consigneeName": "Rhein Parts GmbH", "destinationGln": "%s",
                         "equipmentCode": "CSQU3054383", "sscc": "106141411234567897", "incoterm": "DAP",
                         "origin": "Istanbul Pendik", "destination": "Cologne Eifeltor", "grossWeightKg": 22000,
                         "legs": [{"mode": "ROAD", "origin": "Istanbul Pendik", "destination": "Cologne Eifeltor",
                                   "distanceKm": 2600, "carrierReference": "TR-34-FF-0417"}]}
                        """.formatted(GLN)).with(planner()))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        String id = JsonPath.read(body, "$.id");

        mockMvc.perform(post("/api/v1/tms/shipments/" + id + "/pickup").with(planner())).andExpect(status().isOk());
        mockMvc.perform(post("/api/v1/tms/shipments/" + id + "/departure").with(planner())).andExpect(status().isOk());
        mockMvc.perform(post("/api/v1/tms/shipments/" + id + "/delivery").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"receivedBy\": \"Gate 3\"}").with(planner()))
                .andExpect(status().isOk());

        Map<String, Object> props = KafkaTestUtils.consumerProps(broker, "test-reader", false);
        props.put("auto.offset.reset", "earliest");
        try (Consumer<String, String> consumer = new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(),
                new StringDeserializer()).createConsumer()) {
            consumer.subscribe(java.util.List.of(ShipmentDelivered.TOPIC));
            ConsumerRecord<String, String> record = KafkaTestUtils.getSingleRecord(consumer, ShipmentDelivered.TOPIC,
                    Duration.ofSeconds(15));
            assertThat(record.key()).isEqualTo(id);
            assertThat((String) JsonPath.read(record.value(), "$.tenantId")).isEqualTo("t-kafka");
            assertThat((String) JsonPath.read(record.value(), "$.destinationGln")).isEqualTo(GLN);
            assertThat((String) JsonPath.read(record.value(), "$.sscc")).isEqualTo("106141411234567897");
            EventContract.of(ShipmentDelivered.TOPIC).verify(record.key(), record.value());
        }
        // The pickup and the departure: one message each, the same contract check.
        try (Consumer<String, String> consumer = new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(),
                new StringDeserializer()).createConsumer()) {
            consumer.subscribe(java.util.List.of(ShipmentProgressed.TOPIC));
            ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(15), 2);
            assertThat(records.count()).isEqualTo(2);
            records.forEach(record -> EventContract.of(ShipmentProgressed.TOPIC).verify(record.key(), record.value()));
        }
        // Sent, so the outbox row is completed.
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(jdbc.queryForObject(
                "select count(*) from transportation.event_publication where completion_date is null"
                        + " and serialized_event like ?", Integer.class, "%" + id + "%")).isZero());
    }
}

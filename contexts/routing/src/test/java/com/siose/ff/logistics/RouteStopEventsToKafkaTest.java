package com.siose.ff.logistics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.jayway.jsonpath.JsonPath;
import com.siose.ff.contracts.EventContract;
import com.siose.ff.logistics.routing.RouteStopCompleted;
import java.time.Duration;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
 * Flow 3 leaves the routing service: a completed stop goes through the outbox to its Kafka topic,
 * keyed by the route plan, with the shipment it delivers. Read back from a broker in the test JVM,
 * and checked against its contract (ff-contracts, S06-03), as the modulith's test checks its own.
 */
@SpringBootTest(properties = {
        "spring.modulith.events.externalization.enabled=true",
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        // A database of its own: this context's outbox rows must not mix with the other contexts'.
        "spring.datasource.url=jdbc:h2:mem:routing_kafka;DB_CLOSE_DELAY=-1;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE"
})
@AutoConfigureMockMvc
@EmbeddedKafka
@DirtiesContext
class RouteStopEventsToKafkaTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private EmbeddedKafkaBroker broker;

    @Autowired
    private JdbcTemplate jdbc;

    private static RequestPostProcessor dispatcher() {
        return jwt().jwt(token -> token.subject("dispatcher1").claim("tenant_id", "t-kafka"))
                .authorities(new SimpleGrantedAuthority("ROLE_DISPATCHER"));
    }

    private String post(String path, String body, int expected) throws Exception {
        return mockMvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post(path)
                        .contentType(MediaType.APPLICATION_JSON).content(body).with(dispatcher()))
                .andExpect(status().is(expected))
                .andReturn().getResponse().getContentAsString();
    }

    @Test
    void aCompletedStopReachesItsTopicKeyedByTheRoute() throws Exception {
        post("/api/v1/geo/territories", """
                {"territoryCode": "IST-EU", "name": "Istanbul Europe", "hubGln": "8690001000011",
                 "centerLatitude": 41.05, "centerLongitude": 28.85, "coverageRadiusKm": 35,
                 "activePointCount": 420, "weeklyVisitTarget": 840}
                """, 201);
        String route = post("/api/v1/geo/routes", """
                {"territoryCode": "IST-EU", "vehiclePlate": "34 FF 0417", "depotLatitude": 41.10,
                 "depotLongitude": 28.65, "vehicleCapacityKg": 3500, "averageSpeedKmh": 40, "stops": [
                  {"stopCode": "IST-ESN", "customerName": "Esenyurt Depot Shop", "latitude": 41.03,
                   "longitude": 28.67, "demandWeightKg": 200, "serviceMinutes": 15, "shipmentNumber": "SHP-10042"}]}
                """, 201);
        String routeId = JsonPath.read(route, "$.id");

        post("/api/v1/geo/routes/" + routeId + "/stops/IST-ESN/completion", "{\"receivedBy\": \"Ayse Demir\"}", 200);

        Map<String, Object> props = KafkaTestUtils.consumerProps(broker, "test-reader", false);
        props.put("auto.offset.reset", "earliest");
        try (Consumer<String, String> consumer = new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(),
                new StringDeserializer()).createConsumer()) {
            consumer.subscribe(java.util.List.of(RouteStopCompleted.TOPIC));
            ConsumerRecord<String, String> record = KafkaTestUtils.getSingleRecord(consumer, RouteStopCompleted.TOPIC,
                    Duration.ofSeconds(15));
            assertThat(record.key()).isEqualTo(routeId);
            assertThat((String) JsonPath.read(record.value(), "$.tenantId")).isEqualTo("t-kafka");
            assertThat((String) JsonPath.read(record.value(), "$.shipmentNumber")).isEqualTo("SHP-10042");
            assertThat((String) JsonPath.read(record.value(), "$.receivedBy")).isEqualTo("Ayse Demir");
            EventContract.of(RouteStopCompleted.TOPIC).verify(record.key(), record.value());
        }
        // Sent, so the outbox row is completed.
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(jdbc.queryForObject(
                "select count(*) from routing.event_publication where completion_date is null"
                        + " and serialized_event like ?", Integer.class, "%" + routeId + "%")).isZero());
    }
}

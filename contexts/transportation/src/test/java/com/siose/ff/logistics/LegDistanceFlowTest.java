package com.siose.ff.logistics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.RequestPostProcessor;

/**
 * Flow 1 between the services, through the API as a client sees it, with a stand-in routing
 * service on a local port: what goes out (the path, the planner's token), what comes back, and the
 * estimate when routing answers badly, slowly, or not at all.
 */
@SpringBootTest(properties = "ff.routing-client.read-timeout=PT0.5S")
@AutoConfigureMockMvc
class LegDistanceFlowTest {

    static final String ROAD_BY_ENDS = """
            {"consignorName": "FF Hadimkoy", "consigneeName": "Esenyurt Depot Shop",
             "equipmentCode": "CSQU3054383", "sscc": "106141411234567897", "incoterm": "DAP",
             "origin": "Hadimkoy Hub", "destination": "Esenyurt", "grossWeightKg": 800,
             "legs": [
               {"mode": "ROAD", "origin": "Hadimkoy Hub", "destination": "Esenyurt",
                "originLatitude": 41.10, "originLongitude": 28.65,
                "destinationLatitude": 41.03, "destinationLongitude": 28.67,
                "carrierReference": "TR-34-FF-0417"}]}
            """;

    /** What the stand-in answers next: a status, a body, and how long it takes. */
    record Reply(int status, String body, Duration delay) {
    }

    static final HttpServer ROUTING;
    static final AtomicReference<Reply> REPLY = new AtomicReference<>();
    static final AtomicReference<String> ASKED = new AtomicReference<>();
    static final AtomicReference<String> TOKEN = new AtomicReference<>();

    static {
        try {
            ROUTING = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        ROUTING.createContext("/", exchange -> {
            ASKED.set(exchange.getRequestURI().toString());
            TOKEN.set(exchange.getRequestHeaders().getFirst("Authorization"));
            Reply reply = REPLY.get();
            try {
                Thread.sleep(reply.delay().toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            byte[] body = reply.body().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(reply.status(), body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        // One thread per request: a slow reply must not hold up the next test's request.
        ROUTING.setExecutor(Executors.newCachedThreadPool());
        ROUTING.start();
    }

    @DynamicPropertySource
    static void routingAt(DynamicPropertyRegistry properties) {
        properties.add("ff.routing-client.base-url", () -> "http://127.0.0.1:" + ROUTING.getAddress().getPort());
    }

    @AfterAll
    static void stopRouting() {
        ROUTING.stop(0);
    }

    @Autowired
    private MockMvc mockMvc;

    @BeforeEach
    void routingAnswers() {
        REPLY.set(new Reply(200, "{\"distanceKm\": 12.34}", Duration.ZERO));
        ASKED.set(null);
        TOKEN.set(null);
    }

    private ResultActions plan() throws Exception {
        return mockMvc.perform(post("/api/v1/tms/shipments").contentType(MediaType.APPLICATION_JSON)
                .content(ROAD_BY_ENDS).with(planner()));
    }

    @Test
    void asksRoutingWithThePlannersToken() throws Exception {
        plan().andExpect(status().isCreated())
                .andExpect(jsonPath("$.legs[0].distanceKm").value(12.34))
                .andExpect(jsonPath("$.legs[0].distanceSource").value("ROUTING"));

        assertThat(ASKED.get()).isEqualTo(
                "/api/v1/geo/distance?fromLatitude=41.1&fromLongitude=28.65&toLatitude=41.03&toLongitude=28.67");
        assertThat(TOKEN.get()).isEqualTo("Bearer planner-token");
    }

    @Test
    void anErrorFromRoutingGivesTheEstimate() throws Exception {
        REPLY.set(new Reply(503, "{\"title\": \"Road Matrix Unavailable\"}", Duration.ZERO));

        plan().andExpect(status().isCreated())
                .andExpect(jsonPath("$.legs[0].distanceKm").value(9.87))
                .andExpect(jsonPath("$.legs[0].distanceSource").value("ESTIMATE"));
    }

    @Test
    void aSlowRoutingIsNotWaitedFor() throws Exception {
        REPLY.set(new Reply(200, "{\"distanceKm\": 12.34}", Duration.ofSeconds(3)));
        long started = System.nanoTime();

        plan().andExpect(status().isCreated())
                .andExpect(jsonPath("$.legs[0].distanceSource").value("ESTIMATE"));

        assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(2));
    }

    private static RequestPostProcessor planner() {
        return jwt().jwt(token -> token.tokenValue("planner-token").subject("planner1")
                        .claim("tenant_id", "t-flow1-services"))
                .authorities(new SimpleGrantedAuthority("ROLE_LOGISTICS_PLANNER"));
    }
}

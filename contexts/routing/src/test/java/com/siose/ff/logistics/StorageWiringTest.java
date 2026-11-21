package com.siose.ff.logistics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.jayway.jsonpath.JsonPath;
import com.siose.ff.base.storage.ObjectStoragePort;
import com.siose.ff.base.storage.S3ObjectStorageAdapter;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.RequestPostProcessor;

/**
 * Documents in the routing service (S05-05): with {@code ff.storage.provider=s3}, the S3 adapter of
 * ff-base-storage is the one in use, as in the cluster. No store answers here (port 1), so a link
 * is refused at once with 503, not after a long wait. The links themselves are tested in the
 * areas, and RustFS's write-once in ff-base-storage.
 */
@SpringBootTest(properties = {
        "ff.storage.provider=s3",
        "ff.storage.s3.endpoint=http://127.0.0.1:1",
        "ff.storage.s3.access-key=test",
        "ff.storage.s3.secret-key=test",
        "ff.storage.s3.timeout=PT2S",
        "spring.datasource.url=jdbc:h2:mem:routing_storage;DB_CLOSE_DELAY=-1;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE"
})
@AutoConfigureMockMvc
class StorageWiringTest {

    @Autowired
    private ObjectStoragePort storage;

    @Autowired
    private MockMvc mockMvc;

    private static RequestPostProcessor caller() {
        return jwt().jwt(token -> token.subject("admin1").claim("tenant_id", "t-storage"))
                .authorities(new SimpleGrantedAuthority("ROLE_TENANT_ADMIN"));
    }

    private String post(String path, String body) throws Exception {
        return mockMvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post(path)
                        .contentType(MediaType.APPLICATION_JSON).content(body).with(caller()))
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString();
    }

    @Test
    void usesTheS3StoreAndSaysSoWhenItIsAway() throws Exception {
        assertThat(storage).isInstanceOf(S3ObjectStorageAdapter.class);

        post("/api/v1/geo/territories", """
                {"territoryCode": "IST-EU", "name": "Istanbul Europe", "hubGln": "8690001000011",
                 "centerLatitude": 41.05, "centerLongitude": 28.85, "coverageRadiusKm": 35,
                 "activePointCount": 420, "weeklyVisitTarget": 840}
                """);
        String route = post("/api/v1/geo/routes", """
                {"territoryCode": "IST-EU", "vehiclePlate": "34 FF 0417", "depotLatitude": 41.10,
                 "depotLongitude": 28.65, "vehicleCapacityKg": 3500, "averageSpeedKmh": 40, "stops": [
                  {"stopCode": "IST-ESN", "customerName": "Esenyurt Depot Shop", "latitude": 41.03,
                   "longitude": 28.67, "demandWeightKg": 200, "serviceMinutes": 15}]}
                """);
        mockMvc.perform(post_("/api/v1/geo/routes/" + JsonPath.read(route, "$.id") + "/stops/IST-ESN/epod/upload-link"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.type").value(Matchers.endsWith("object-store-unavailable")));
    }

    private org.springframework.test.web.servlet.RequestBuilder post_(String path) {
        return org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post(path).with(caller());
    }
}

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
 * Documents in the transportation service (S05-05): with {@code ff.storage.provider=s3}, the S3 adapter of
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
        "spring.datasource.url=jdbc:h2:mem:transportation_storage;DB_CLOSE_DELAY=-1;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE"
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

        String shipment = post("/api/v1/tms/shipments", """
                {"consignorName": "FF Hadimkoy", "consigneeName": "Esenyurt Depot Shop",
                 "equipmentCode": "CSQU3054383", "sscc": "106141411234567897", "incoterm": "DAP",
                 "origin": "Hadimkoy Hub", "destination": "Esenyurt", "grossWeightKg": 800,
                 "legs": [{"mode": "ROAD", "origin": "Hadimkoy Hub", "destination": "Esenyurt",
                           "distanceKm": 9.87, "carrierReference": "TR-34-FF-0417"}]}
                """);
        mockMvc.perform(post_("/api/v1/tms/shipments/" + JsonPath.read(shipment, "$.id") + "/ecmr/upload-link"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.type").value(Matchers.endsWith("object-store-unavailable")));
    }

    private org.springframework.test.web.servlet.RequestBuilder post_(String path) {
        return org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post(path).with(caller());
    }
}

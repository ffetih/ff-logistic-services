package com.siose.ff.logistics;

import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.siose.ff.logistics.standards.Gs1Standards;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.RequestPostProcessor;

/**
 * The warehousing area is served by the warehousing service. Its rules are tested once, in the area's own
 * tests (ff-domain-warehousing); this only checks what the application adds around them: the
 * route, the role check, the tenant from the token, and request validation.
 */
@SpringBootTest
@AutoConfigureMockMvc
class WarehousingServedTest {

    private static final String PREFIX = "8690002";

    @Autowired
    private MockMvc mockMvc;

    @Test
    void createsAndListsAWarehouseForTheCallersTenant() throws Exception {
        String gln = PREFIX + "00001" + Gs1Standards.checkDigit(PREFIX + "00001");
        String body = """
                {"code": "IST-SMOKE", "name": "Smoke test hub", "gln": "%s", "gs1CompanyPrefix": "%s",
                 "countryCode": "TR", "city": "Istanbul", "latitude": 41.1, "longitude": 28.6,
                 "palletCapacity": 100}
                """.formatted(gln, PREFIX);

        mockMvc.perform(post("/api/v1/wms/warehouses").contentType(MediaType.APPLICATION_JSON).content(body)
                        .with(operator("t-served")))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.gln").value(gln));
        mockMvc.perform(get("/api/v1/wms/warehouses").with(operator("t-served")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].code").value("IST-SMOKE"));
    }

    @Test
    void checksTheRoleAndTheRequest() throws Exception {
        mockMvc.perform(get("/api/v1/wms/warehouses")).andExpect(status().isUnauthorized());
        mockMvc.perform(get("/api/v1/wms/warehouses")
                        .with(jwt().jwt(token -> token.claim("tenant_id", "t-served"))))
                .andExpect(status().isForbidden());
        mockMvc.perform(post("/api/v1/wms/warehouses").contentType(MediaType.APPLICATION_JSON).content("{}")
                        .with(operator("t-served")))
                .andExpect(status().isBadRequest());
    }

    private static RequestPostProcessor operator(String tenant) {
        return jwt().jwt(token -> token.subject("operator1").claim("tenant_id", tenant))
                .authorities(new SimpleGrantedAuthority("ROLE_WAREHOUSE_OPERATOR"));
    }
}

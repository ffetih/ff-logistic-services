package com.siose.ff.logistics;

import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

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
 * The routing area is served by the routing service. Its rules are tested once, in the area's own tests
 * (ff-domain-routing); this only checks what the application adds around them: the route, the role
 * check, the tenant from the token, request validation, and a road matrix to plan with (the
 * straight-line estimate, the default).
 */
@SpringBootTest
@AutoConfigureMockMvc
class RoutingServedTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void plansARouteInATerritoryOfTheCallersTenant() throws Exception {
        mockMvc.perform(post("/api/v1/geo/territories").contentType(MediaType.APPLICATION_JSON).content("""
                        {"territoryCode": "IST-EU", "name": "Istanbul Europe", "hubGln": "8690123450015",
                         "centerLatitude": 41.05, "centerLongitude": 28.85, "coverageRadiusKm": 35,
                         "activePointCount": 420, "weeklyVisitTarget": 840}
                        """).with(dispatcher("t-routing")))
                .andExpect(status().isCreated());

        mockMvc.perform(post("/api/v1/geo/routes").contentType(MediaType.APPLICATION_JSON).content("""
                        {"territoryCode": "IST-EU", "vehiclePlate": "34 FF 0417", "depotLatitude": 41.10,
                         "depotLongitude": 28.65, "vehicleCapacityKg": 3500, "averageSpeedKmh": 40, "stops": [
                          {"stopCode": "IST-KDK", "customerName": "Kadikoy Market", "latitude": 40.99,
                           "longitude": 29.03, "demandWeightKg": 400, "serviceMinutes": 15},
                          {"stopCode": "IST-ESN", "customerName": "Esenyurt Depot Shop", "latitude": 41.03,
                           "longitude": 28.67, "demandWeightKg": 200, "serviceMinutes": 15}]}
                        """).with(dispatcher("t-routing")))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.routeNumber").value(Matchers.startsWith("RTE-")))
                .andExpect(jsonPath("$.tenantId").value("t-routing"))
                .andExpect(jsonPath("$.stops[*].stopCode").value(Matchers.contains("IST-ESN", "IST-KDK")));
    }

    @Test
    void checksTheRoleAndTheRequest() throws Exception {
        mockMvc.perform(get("/api/v1/geo/routes")).andExpect(status().isUnauthorized());
        mockMvc.perform(get("/api/v1/geo/routes")
                        .with(jwt().jwt(token -> token.claim("tenant_id", "t-routing"))))
                .andExpect(status().isForbidden());
        mockMvc.perform(post("/api/v1/geo/routes").contentType(MediaType.APPLICATION_JSON).content("{}")
                        .with(dispatcher("t-routing")))
                .andExpect(status().isBadRequest());
    }

    private static RequestPostProcessor dispatcher(String tenant) {
        return jwt().jwt(token -> token.subject("dispatcher1").claim("tenant_id", tenant))
                .authorities(new SimpleGrantedAuthority("ROLE_DISPATCHER"));
    }
}

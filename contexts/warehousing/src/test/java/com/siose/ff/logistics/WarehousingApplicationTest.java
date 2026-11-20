package com.siose.ff.logistics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.siose.ff.base.persistence.AreaMigrations;
import com.siose.ff.base.persistence.MigrationArea;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;

/** The walking skeleton of one service: its one area migrated, the probes, a token's caller. */
@SpringBootTest
@AutoConfigureMockMvc
class WarehousingApplicationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private AreaMigrations migrations;

    @Test
    void holdsOnlyItsOwnAreaAndMigratesIt() {
        assertThat(migrations.areas()).extracting(MigrationArea::name).containsExactly("warehousing");
        Integer version = jdbc.queryForObject(
                "select max(\"version\") from \"warehousing_flyway_history\" where \"type\" = 'SQL'", Integer.class);
        assertThat(version).isEqualTo(3);
    }

    @Test
    void answersTheProbesAndTheReleaseGroup() throws Exception {
        for (String path : new String[] {"/actuator/health/liveness", "/actuator/health/readiness",
                "/actuator/health/release"}) {
            mockMvc.perform(get(path)).andExpect(status().isOk()).andExpect(jsonPath("$.status").value("UP"));
        }
    }

    @Test
    void saysWhoIsCallingOnlyWithAToken() throws Exception {
        mockMvc.perform(get("/api/v1/me")).andExpect(status().isUnauthorized());
        mockMvc.perform(get("/api/v1/me").with(jwt().jwt(token -> token
                        .subject("dispatcher1").claim("tenant_id", "ff-eu-freight"))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.tenantId").value("ff-eu-freight"));
    }
}

package com.devflowai.security;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("production")
@TestPropertySource(properties = {
        "spring.datasource.url=jdbc:h2:mem:devflow-security-test;MODE=MySQL;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.jpa.hibernate.ddl-auto=create-drop",
        "spring.flyway.enabled=false"
})
class ProductionTenantSecurityTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void arbitraryTenantHeaderDoesNotAuthenticateProductionRequests() throws Exception {
        mockMvc.perform(get("/api/v1/jobs").header("X-Tenant-Id", "attacker-chosen-tenant"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void readinessProbeIsAccessibleWithoutApplicationCredentials() throws Exception {
        mockMvc.perform(get("/actuator/health/readiness"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("UP"));
    }
}

package com.devflowai.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("development")
@TestPropertySource(properties = {
        "spring.datasource.url=jdbc:h2:mem:devflow-api-test;MODE=MySQL;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.jpa.hibernate.ddl-auto=create-drop",
        "spring.flyway.enabled=false"
})
class JobApiIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void submissionRetrievalListingAndHistoryUseDevelopmentTenantIdentity() throws Exception {
        String response = mockMvc.perform(post("/api/v1/jobs")
                        .header("X-Tenant-Id", "tenant-a")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "tenantId": "tenant-b",
                                  "jobType": "report",
                                  "payload": "{}",
                                  "priority": "HIGH",
                                  "resourceUnits": 2,
                                  "maxAttempts": 3,
                                  "idempotencyKey": "http-happy-path"
                                }
                                """))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.tenantId").value("tenant-a"))
                .andExpect(jsonPath("$.status").value("QUEUED"))
                .andReturn().getResponse().getContentAsString();

        JsonNode submitted = objectMapper.readTree(response);
        long jobId = submitted.get("id").asLong();

        mockMvc.perform(get("/api/v1/jobs/{jobId}", jobId)
                        .header("X-Tenant-Id", "tenant-a"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(jobId))
                .andExpect(jsonPath("$.tenantId").value("tenant-a"));

        mockMvc.perform(get("/api/v1/jobs")
                        .header("X-Tenant-Id", "tenant-a")
                        .param("tenant", "tenant-b")
                        .param("status", "queued")
                        .param("priority", "high")
                        .param("page", "0")
                        .param("size", "10"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalElements").value(1))
                .andExpect(jsonPath("$.jobs[0].tenantId").value("tenant-a"));

        mockMvc.perform(get("/api/v1/jobs/{jobId}/history", jobId)
                        .header("X-Tenant-Id", "tenant-a"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2))
                .andExpect(jsonPath("$[0].status").value("ACCEPTED"))
                .andExpect(jsonPath("$[1].status").value("QUEUED"));
    }

    @Test
    void validatesSubmissionAndReturnsExpectedApiErrors() throws Exception {
        mockMvc.perform(post("/api/v1/jobs")
                        .header("X-Tenant-Id", "tenant-a")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "jobType": " ",
                                  "payload": "{}",
                                  "priority": "NORMAL",
                                  "resourceUnits": 0,
                                  "maxAttempts": 2,
                                  "idempotencyKey": "invalid-http"
                                }
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("Validation failed"))
                .andExpect(jsonPath("$.details.jobType").exists())
                .andExpect(jsonPath("$.details.resourceUnits").exists());

        mockMvc.perform(get("/api/v1/jobs/999999")
                        .header("X-Tenant-Id", "tenant-a"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error").value("Job not found"));

        String response = mockMvc.perform(post("/api/v1/jobs")
                        .header("X-Tenant-Id", "tenant-a")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "jobType": "task",
                                  "payload": "{}",
                                  "priority": "NORMAL",
                                  "resourceUnits": 1,
                                  "maxAttempts": 2,
                                  "idempotencyKey": "retry-conflict"
                                }
                                """))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        long jobId = objectMapper.readTree(response).get("id").asLong();

        mockMvc.perform(post("/api/v1/jobs/{jobId}/retry", jobId)
                        .header("X-Tenant-Id", "tenant-a"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.error").value("Invalid state transition"));

        mockMvc.perform(get("/api/v1/jobs")
                        .header("X-Tenant-Id", "tenant-a")
                        .param("status", "not-a-status"))
                .andExpect(status().isBadRequest());

        mockMvc.perform(get("/api/v1/jobs")
                        .header("X-Tenant-Id", "tenant-a")
                        .param("size", "101"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void cannotRetrieveAnotherDevelopmentTenantsJob() throws Exception {
        String response = mockMvc.perform(post("/api/v1/jobs")
                        .header("X-Tenant-Id", "tenant-a")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "jobType": "task",
                                  "payload": "{}",
                                  "priority": "NORMAL",
                                  "resourceUnits": 1,
                                  "maxAttempts": 2,
                                  "idempotencyKey": "tenant-visibility"
                                }
                                """))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        long jobId = objectMapper.readTree(response).get("id").asLong();

        mockMvc.perform(get("/api/v1/jobs/{jobId}", jobId)
                        .header("X-Tenant-Id", "tenant-b"))
                .andExpect(status().isNotFound());

        mockMvc.perform(get("/api/v1/jobs").header("X-Tenant-Id", "tenant-b"))
                .andExpect(status().isOk())
                .andExpect(result -> assertEquals(0,
                        objectMapper.readTree(result.getResponse().getContentAsString())
                                .get("totalElements").asInt()));
    }

    @Test
    void cancelsQueuedJobOverHttp() throws Exception {
        String response = mockMvc.perform(post("/api/v1/jobs")
                        .header("X-Tenant-Id", "tenant-cancel")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "jobType": "cancel-me",
                                  "payload": "{}",
                                  "priority": "NORMAL",
                                  "resourceUnits": 1,
                                  "maxAttempts": 2,
                                  "idempotencyKey": "http-cancel"
                                }
                                """))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        long jobId = objectMapper.readTree(response).get("id").asLong();

        mockMvc.perform(post("/api/v1/jobs/{jobId}/cancel", jobId)
                        .header("X-Tenant-Id", "tenant-cancel"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("CANCELLED"));
    }
}

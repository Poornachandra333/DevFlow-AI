package com.devflowai.api;

import com.devflowai.api.dto.PagedJobsResponse;
import com.devflowai.job.JobPriority;
import com.devflowai.job.JobStatus;
import com.devflowai.security.TenantContext;
import com.devflowai.service.JobService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.any;

@ExtendWith(MockitoExtension.class)
class JobControllerTest {

    @Mock
    private JobService jobService;

    @InjectMocks
    private JobController jobController;

    @BeforeEach
    void setUp() {
        TenantContext.setTenantId("tenant-a");
    }

    @AfterEach
    void tearDown() {
        TenantContext.clear();
    }

    @Test
    void listJobsUsesTenantContextAndRequestedFiltersAndPage() {
        PageRequest pageable = PageRequest.of(1, 7);
        when(jobService.listJobs(eq("tenant-a"), eq(JobStatus.QUEUED), eq(JobPriority.HIGH), any()))
                .thenReturn(new PageImpl<>(List.of(), pageable, 8));

        PagedJobsResponse response = jobController.listJobs("queued", "high", 1, 7);

        verify(jobService).listJobs("tenant-a", JobStatus.QUEUED, JobPriority.HIGH, pageable);
        assertEquals(1, response.getPage());
        assertEquals(2, response.getTotalPages());
        assertEquals(8, response.getTotalElements());
    }
}

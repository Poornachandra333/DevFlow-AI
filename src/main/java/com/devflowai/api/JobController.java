package com.devflowai.api;

import com.devflowai.api.dto.JobResponse;
import com.devflowai.api.dto.JobSubmissionRequest;
import com.devflowai.api.dto.JobSummaryResponse;
import com.devflowai.api.dto.PagedJobsResponse;
import com.devflowai.job.JobPriority;
import com.devflowai.job.JobStatus;
import com.devflowai.security.TenantContext;
import com.devflowai.service.JobService;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/v1")
@Tag(name = "Jobs", description = "Job submission, retrieval, retry and cancellation APIs")
public class JobController {

    private final JobService jobService;

    public JobController(JobService jobService) {
        this.jobService = jobService;
    }

    @PostMapping("/jobs")
    @Operation(summary = "Submit a new job",
            description = "Creates a tenant-scoped job and requests scheduling after the database transaction commits.")
    @ApiResponses({
            @ApiResponse(responseCode = "201", description = "Job accepted"),
            @ApiResponse(responseCode = "400", description = "Request validation failed"),
            @ApiResponse(responseCode = "409", description = "Tenant quota or idempotency conflict"),
            @ApiResponse(responseCode = "429", description = "Tenant submission rate limit exceeded"),
            @ApiResponse(responseCode = "503", description = "Rate limit service unavailable")
    })
    public ResponseEntity<JobResponse> submitJob(@Valid @RequestBody JobSubmissionRequest request) {
        String tenantId = TenantContext.getTenantId();
        JobResponse response = jobService.submitJob(tenantId, request);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    @GetMapping("/jobs/{jobId}")
    @Operation(summary = "Get a job by ID", description = "Retrieves a job belonging to the resolved tenant.")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Job found"),
            @ApiResponse(responseCode = "404", description = "Job not found for this tenant")
    })
    public JobResponse getJob(@PathVariable Long jobId) {
        return jobService.getJobById(TenantContext.getTenantId(), jobId);
    }

    @GetMapping("/jobs")
    @Operation(summary = "List jobs with filters and pagination",
            description = "Lists jobs for the resolved tenant. Tenant identity is never selected by a query parameter.")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Page of jobs"),
            @ApiResponse(responseCode = "400", description = "Invalid filter or pagination parameter")
    })
    public PagedJobsResponse listJobs(
            @Parameter(description = "Optional job status filter") @RequestParam(required = false) String status,
            @Parameter(description = "Optional job priority filter") @RequestParam(required = false) String priority,
            @Parameter(description = "Zero-based page index") @RequestParam(defaultValue = "0") int page,
            @Parameter(description = "Page size, from 1 to 100") @RequestParam(defaultValue = "20") int size) {

        if (page < 0) {
            throw new IllegalArgumentException("page must be zero or greater");
        }
        if (size < 1 || size > 100) {
            throw new IllegalArgumentException("size must be between 1 and 100");
        }

        JobStatus jobStatus = status == null || status.isBlank() ? null : JobStatus.valueOf(status.trim().toUpperCase());
        JobPriority jobPriority = priority == null || priority.isBlank() ? null : JobPriority.valueOf(priority.trim().toUpperCase());
        String tenantId = TenantContext.getTenantId();

        Pageable pageable = PageRequest.of(page, size);
        Page<JobSummaryResponse> pageResult = jobService.listJobs(tenantId, jobStatus, jobPriority, pageable);
        return new PagedJobsResponse(pageResult.getContent(), pageResult.getNumber(), pageResult.getTotalPages(), pageResult.getTotalElements());
    }

    @GetMapping("/jobs/{jobId}/history")
    @Operation(summary = "Get execution history for a job")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Job execution history"),
            @ApiResponse(responseCode = "404", description = "Job not found for this tenant")
    })
    public List<JobResponse> getExecutionHistory(@PathVariable Long jobId) {
        return jobService.getExecutionHistory(TenantContext.getTenantId(), jobId);
    }

    @PostMapping("/jobs/{jobId}/cancel")
    @Operation(summary = "Cancel an eligible job")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Job cancelled"),
            @ApiResponse(responseCode = "404", description = "Job not found for this tenant"),
            @ApiResponse(responseCode = "409", description = "Job cannot be cancelled from its current state")
    })
    public JobResponse cancelJob(@PathVariable Long jobId) {
        return jobService.cancelJob(TenantContext.getTenantId(), jobId);
    }

    @PostMapping("/jobs/{jobId}/retry")
    @Operation(summary = "Retry a failed job if eligible")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Retry scheduled"),
            @ApiResponse(responseCode = "404", description = "Job not found for this tenant"),
            @ApiResponse(responseCode = "409", description = "Job is not retryable")
    })
    public JobResponse retryJob(@PathVariable Long jobId) {
        return jobService.retryJob(TenantContext.getTenantId(), jobId);
    }
}

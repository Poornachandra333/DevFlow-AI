package com.devflowai.repository;

import com.devflowai.domain.JobEntity;
import com.devflowai.job.JobPriority;
import com.devflowai.job.JobStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.data.jpa.repository.Lock;

import java.util.List;
import java.util.Optional;
import jakarta.persistence.LockModeType;

public interface JobRepository extends JpaRepository<JobEntity, Long> {

    Optional<JobEntity> findByTenantIdAndId(String tenantId, Long id);

    @Query("select j from JobEntity j where j.tenantId = :tenantId and (:status is null or j.status = :status) and (:priority is null or j.priority = :priority)")
    Page<JobEntity> findByTenantAndFilters(@Param("tenantId") String tenantId,
                                          @Param("status") JobStatus status,
                                          @Param("priority") JobPriority priority,
                                          Pageable pageable);

    @Query("select j from JobEntity j where j.tenantId = :tenantId and j.id = :jobId")
    Optional<JobEntity> findOwnedJob(@Param("tenantId") String tenantId, @Param("jobId") Long jobId);

    @Query("select j from JobEntity j where j.tenantId = :tenantId and j.idempotencyKey = :idempotencyKey")
    Optional<JobEntity> findByTenantAndIdempotencyKey(@Param("tenantId") String tenantId, @Param("idempotencyKey") String idempotencyKey);

    long countByTenantIdAndStatusIn(String tenantId, List<JobStatus> statuses);

    @Query("select coalesce(sum(j.resourceUnits), 0) from JobEntity j where j.tenantId = :tenantId and j.status in :statuses")
    long sumResourceUnitsByTenantAndStatusIn(@Param("tenantId") String tenantId,
                                             @Param("statuses") List<JobStatus> statuses);

    long countByTenantId(String tenantId);

    @Query("select j from JobEntity j where j.tenantId = :tenantId and j.status in :statuses order by j.priority desc, j.createdAt asc")
    List<JobEntity> findQueuedForTenant(@Param("tenantId") String tenantId, @Param("statuses") List<JobStatus> statuses);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select j from JobEntity j where j.id = :jobId")
    Optional<JobEntity> findByIdForUpdate(@Param("jobId") Long jobId);

    @Query("select j from JobEntity j where j.status in :statuses and (j.status <> com.devflowai.job.JobStatus.RETRY_SCHEDULED or j.scheduledAt <= :now) order by j.createdAt asc")
    List<JobEntity> findDispatchCandidates(@Param("statuses") List<JobStatus> statuses,
                                          @Param("now") java.time.Instant now,
                                          org.springframework.data.domain.Pageable pageable);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select j from JobEntity j where j.status = com.devflowai.job.JobStatus.RUNNING and j.leaseExpiresAt <= :now")
    List<JobEntity> findExpiredLeasesForUpdate(@Param("now") java.time.Instant now,
                                               org.springframework.data.domain.Pageable pageable);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select j from JobEntity j where j.status = com.devflowai.job.JobStatus.RETRY_SCHEDULED and j.scheduledAt <= :now")
    List<JobEntity> findDueRetriesForUpdate(@Param("now") java.time.Instant now,
                                            org.springframework.data.domain.Pageable pageable);

    long countByStatus(com.devflowai.job.JobStatus status);

    List<JobEntity> findByStatus(com.devflowai.job.JobStatus status);

    @Query("""
            select j.tenantId as tenantId, count(j) as runningCount,
                   coalesce(sum(j.resourceUnits), 0) as runningResourceUnits
            from JobEntity j
            where j.status = com.devflowai.job.JobStatus.RUNNING
            group by j.tenantId
            """)
    List<RunningJobUsage> findRunningUsageByTenant();

    long countByTenantIdAndStatus(String tenantId, com.devflowai.job.JobStatus status);

    @Query("select coalesce(sum(j.resourceUnits), 0) from JobEntity j where j.tenantId = :tenantId and j.status = com.devflowai.job.JobStatus.RUNNING")
    long sumRunningResourceUnitsByTenant(@Param("tenantId") String tenantId);

    interface RunningJobUsage {
        String getTenantId();
        long getRunningCount();
        long getRunningResourceUnits();
    }
}

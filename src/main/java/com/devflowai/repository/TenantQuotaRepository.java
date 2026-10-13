package com.devflowai.repository;

import com.devflowai.domain.TenantQuota;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;
import java.util.List;
import jakarta.persistence.LockModeType;

public interface TenantQuotaRepository extends JpaRepository<TenantQuota, Long> {
    Optional<TenantQuota> findByTenantId(String tenantId);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select q from TenantQuota q where q.tenantId = :tenantId")
    Optional<TenantQuota> findByTenantIdForUpdate(@Param("tenantId") String tenantId);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select q from TenantQuota q order by q.tenantId")
    List<TenantQuota> findAllForUpdate();
}

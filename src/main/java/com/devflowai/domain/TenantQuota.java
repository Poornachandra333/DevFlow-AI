package com.devflowai.domain;

import jakarta.persistence.*;
import java.time.Instant;

@Entity
@Table(name = "tenant_quotas", uniqueConstraints = {
        @UniqueConstraint(name = "uk_tenant_quotas_tenant", columnNames = "tenant_id")
})
public class TenantQuota {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "tenant_id", nullable = false, length = 100)
    private String tenantId;

    @Column(name = "max_concurrent_jobs", nullable = false)
    private int maxConcurrentJobs = 5;

    @Column(name = "max_resource_units", nullable = false)
    private int maxResourceUnits = 50;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt = Instant.now();

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt = Instant.now();

    protected TenantQuota() {
    }

    public TenantQuota(String tenantId, int maxConcurrentJobs, int maxResourceUnits) {
        this.tenantId = tenantId;
        this.maxConcurrentJobs = maxConcurrentJobs;
        this.maxResourceUnits = maxResourceUnits;
    }

    public Long getId() {
        return id;
    }

    public String getTenantId() {
        return tenantId;
    }

    public int getMaxConcurrentJobs() {
        return maxConcurrentJobs;
    }

    public int getMaxResourceUnits() {
        return maxResourceUnits;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }
}

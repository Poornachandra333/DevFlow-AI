CREATE TABLE IF NOT EXISTS tenant_quotas (
    id BIGINT NOT NULL AUTO_INCREMENT,
    tenant_id VARCHAR(100) NOT NULL,
    max_concurrent_jobs INT NOT NULL DEFAULT 5,
    max_resource_units INT NOT NULL DEFAULT 50,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_tenant_quotas_tenant (tenant_id)
);

CREATE TABLE IF NOT EXISTS jobs (
    id BIGINT NOT NULL AUTO_INCREMENT,
    tenant_id VARCHAR(100) NOT NULL,
    job_type VARCHAR(120) NOT NULL,
    status VARCHAR(30) NOT NULL,
    priority VARCHAR(20) NOT NULL,
    resource_units INT NOT NULL,
    max_attempts INT NOT NULL DEFAULT 3,
    attempt_count INT NOT NULL DEFAULT 0,
    payload VARCHAR(8000) NOT NULL,
    idempotency_key VARCHAR(255) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    scheduled_at TIMESTAMP NULL,
    started_at TIMESTAMP NULL,
    completed_at TIMESTAMP NULL,
    failure_reason VARCHAR(1000) NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_jobs_tenant_idempotency (tenant_id, idempotency_key),
    KEY idx_jobs_tenant_created (tenant_id, created_at),
    KEY idx_jobs_status_priority (status, priority),
    KEY idx_jobs_idempotency (tenant_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS job_execution_history (
    id BIGINT NOT NULL AUTO_INCREMENT,
    job_id BIGINT NOT NULL,
    status VARCHAR(30) NOT NULL,
    message VARCHAR(1000) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    KEY idx_job_exec_job_id (job_id),
    KEY idx_job_exec_status (status),
    CONSTRAINT fk_job_exec_job FOREIGN KEY (job_id) REFERENCES jobs(id)
);

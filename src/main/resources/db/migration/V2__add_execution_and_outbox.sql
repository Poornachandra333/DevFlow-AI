ALTER TABLE jobs
    ADD COLUMN correlation_id VARCHAR(36) NULL,
    ADD COLUMN result LONGTEXT NULL,
    ADD COLUMN lease_token VARCHAR(36) NULL,
    ADD COLUMN lease_expires_at TIMESTAMP NULL;

UPDATE jobs
SET correlation_id = UUID()
WHERE correlation_id IS NULL;

ALTER TABLE jobs
    MODIFY COLUMN correlation_id VARCHAR(36) NOT NULL;

CREATE INDEX idx_jobs_dispatch ON jobs (status, scheduled_at, created_at);
CREATE INDEX idx_jobs_lease_expiry ON jobs (status, lease_expires_at);

CREATE TABLE outbox_events (
    event_id VARCHAR(36) NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    schema_version INT NOT NULL,
    job_id BIGINT NOT NULL,
    correlation_id VARCHAR(36) NOT NULL,
    payload LONGTEXT NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    attempt_count INT NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    next_attempt_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    lease_owner VARCHAR(36) NULL,
    lease_expires_at TIMESTAMP NULL,
    published_at TIMESTAMP NULL,
    last_error VARCHAR(2000) NULL,
    PRIMARY KEY (event_id),
    KEY idx_outbox_status_next_attempt (status, next_attempt_at),
    KEY idx_outbox_job_id (job_id),
    CONSTRAINT fk_outbox_job FOREIGN KEY (job_id) REFERENCES jobs(id)
);

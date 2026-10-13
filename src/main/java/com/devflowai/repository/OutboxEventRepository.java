package com.devflowai.repository;

import com.devflowai.domain.OutboxEvent;
import jakarta.persistence.LockModeType;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, String> {
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("""
            select e from OutboxEvent e
            where (e.status = com.devflowai.domain.OutboxEvent.Status.PENDING and e.nextAttemptAt <= :now)
               or (e.status = com.devflowai.domain.OutboxEvent.Status.IN_FLIGHT and e.leaseExpiresAt <= :now)
            order by e.createdAt asc
            """)
    List<OutboxEvent> findAvailableForUpdate(@Param("now") Instant now, Pageable pageable);

    long countByStatus(OutboxEvent.Status status);
}

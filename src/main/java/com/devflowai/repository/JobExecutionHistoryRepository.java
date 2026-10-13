package com.devflowai.repository;

import com.devflowai.domain.JobExecutionHistory;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface JobExecutionHistoryRepository extends JpaRepository<JobExecutionHistory, Long> {
    List<JobExecutionHistory> findByJobIdOrderByCreatedAtAsc(Long jobId);
}

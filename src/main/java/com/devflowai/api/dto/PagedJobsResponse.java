package com.devflowai.api.dto;

import java.util.List;

public class PagedJobsResponse {
    private List<JobSummaryResponse> jobs;
    private int page;
    private int totalPages;
    private long totalElements;

    public PagedJobsResponse() {
    }

    public PagedJobsResponse(List<JobSummaryResponse> jobs, int page, int totalPages, long totalElements) {
        this.jobs = jobs;
        this.page = page;
        this.totalPages = totalPages;
        this.totalElements = totalElements;
    }

    public List<JobSummaryResponse> getJobs() { return jobs; }
    public void setJobs(List<JobSummaryResponse> jobs) { this.jobs = jobs; }
    public int getPage() { return page; }
    public void setPage(int page) { this.page = page; }
    public int getTotalPages() { return totalPages; }
    public void setTotalPages(int totalPages) { this.totalPages = totalPages; }
    public long getTotalElements() { return totalElements; }
    public void setTotalElements(long totalElements) { this.totalElements = totalElements; }
}

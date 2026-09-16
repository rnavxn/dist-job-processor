package com.example.jobqueue.dist_job_processor.service;

import com.example.jobqueue.dist_job_processor.BaseIntegrationTest;
import com.example.jobqueue.dist_job_processor.config.JobConstants;
import com.example.jobqueue.dist_job_processor.entity.JobEntity;
import com.example.jobqueue.dist_job_processor.model.JobStatus;
import com.example.jobqueue.dist_job_processor.model.JobType;
import com.example.jobqueue.dist_job_processor.repository.JobRepository;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class WorkerServiceTest extends BaseIntegrationTest {

    @Autowired
    private ProducerService producerService;

    @Autowired
    private JobRepository jobRepository;

    @BeforeAll
    public static void setupConfig() {
        // Speed up the tests by eliminating random failures and making the simulated task extremely fast
        JobConstants.SIMULATED_FAILURE_RATE = 0.0;
        JobConstants.SIMULATED_TASK_MIN_MS = 10;
        JobConstants.SIMULATED_TASK_MAX_MS = 20;
    }

    @AfterAll
    public static void resetConfig() {
        JobConstants.SIMULATED_FAILURE_RATE = 0.2;
        JobConstants.SIMULATED_TASK_MIN_MS = 500;
        JobConstants.SIMULATED_TASK_MAX_MS = 4000;
    }

    @Test
    public void testEndToEndJobProcessing() {
        // 1. Enqueue a new job
        var response = producerService.enqueue(JobType.EMAIL_SEND, "e2e-test-payload", null, null);
        String jobId = response.getId();

        // 2. Awaitility will poll until the condition is met (WorkerService processes the job)
        // Since both Producer and Worker profiles are active, a worker will automatically pick this up from Redis.
        await()
            .atMost(10, TimeUnit.SECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> {
                Optional<JobEntity> jobOpt = jobRepository.findById(jobId);
                assertTrue(jobOpt.isPresent(), "Job should exist in database");
                
                // Assert that the WorkerService successfully marked it as COMPLETED
                assertEquals(JobStatus.COMPLETED, jobOpt.get().getStatus(), "Job should be completed by worker");
            });
    }
}

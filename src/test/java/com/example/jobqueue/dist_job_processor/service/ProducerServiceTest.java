package com.example.jobqueue.dist_job_processor.service;

import com.example.jobqueue.dist_job_processor.BaseIntegrationTest;
import com.example.jobqueue.dist_job_processor.DTO.JobResponse;
import com.example.jobqueue.dist_job_processor.model.JobType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class ProducerServiceTest extends BaseIntegrationTest {

    @Autowired
    private ProducerService producerService;

    @Test
    public void testIdempotency_GeneratesUniqueKeysWhenNotProvided() {
        JobResponse job1 = producerService.enqueue(JobType.EMAIL_SEND, "payload", null, null);
        JobResponse job2 = producerService.enqueue(JobType.EMAIL_SEND, "payload", null, null);

        assertNotEquals(job1.getId(), job2.getId(), "Jobs with identical payloads should generate unique IDs if key is not provided");
    }

    @Test
    public void testIdempotency_RejectsDuplicatesWhenKeyIsProvided() {
        String idempotencyKey = "test-key-123";
        JobResponse job1 = producerService.enqueue(JobType.EMAIL_SEND, "payload", idempotencyKey, null);
        JobResponse job2 = producerService.enqueue(JobType.EMAIL_SEND, "payload", idempotencyKey, null);

        assertEquals(job1.getId(), job2.getId(), "Second submission with same key should return the exact same job ID");
    }
}

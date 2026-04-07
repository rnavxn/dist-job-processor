package com.example.jobqueue.dist_job_processor.metrics;

import com.example.jobqueue.dist_job_processor.redis.RedisKeys;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.Data;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

@Data
@Service
public class JobMetrics {
    private final Counter jobsEnqueued;
    private final Counter jobsCompleted;
    private final Counter jobsFailed;
    private final Counter jobsRetried;
    private final Counter jobsMovedToDlq;
    private final Timer jobProcessingTime;

    private final Counter jobsRecovered;        // Reaper reclaims
    private final Counter reconciliationMissing;  // Missing from Redis
    private final Counter reconciliationCorrupted; // Corrupted metadata


    public JobMetrics(MeterRegistry registry, JedisPool jedisPool) {
        this.jobsEnqueued = Counter.builder("job.enqueued.total")
                .description("Total number of jobs enqueued")
                .register(registry);
        this.jobsCompleted = Counter.builder("job.completed.total")
                .description("Total number of jobs completed successfully")
                .register(registry);
        this.jobsFailed = Counter.builder("job.failed.total")
                .description("Total number of jobs that failed")
                .register(registry);
        this.jobsRetried = Counter.builder("job.retried.total")
                .description("Total number of job retries")
                .register(registry);
        this.jobsMovedToDlq = Counter.builder("job.dlq.total")
                .description("Total number of jobs moved to Dead Letter Queue")
                .register(registry);
        this.jobProcessingTime = Timer.builder("job.processing.time")
                .description("Time taken to process a job")
                .register(registry);

        this.jobsRecovered = Counter.builder("job.recovered.total")
                .description("Jobs recovered by Reaper (worker crashes)")
                .register(registry);

        this.reconciliationMissing = Counter.builder("reconciliation.missing.jobs")
                .description("Jobs missing from Redis, restored by reconciliation")
                .register(registry);

        this.reconciliationCorrupted = Counter.builder("reconciliation.corrupted.metadata")
                .description("Jobs with corrupted metadata, fixed by reconciliation")
                .register(registry);


        Gauge.builder("job.queue.depth", jedisPool, pool -> {
            try (Jedis jedis = pool.getResource()) {
                return (double) jedis.llen(RedisKeys.JOB_QUEUE);
            } catch (Exception e) {
                return 0.0;
            }
        }).description("Jobs waiting in main queue").register(registry);

        Gauge.builder("job.retry.queue.depth", jedisPool, pool -> {
            try (Jedis jedis = pool.getResource()) {
                return (double) jedis.zcard(RedisKeys.RETRY_QUEUE);
            } catch (Exception e) {
                return 0.0;
            }
        }).description("Jobs waiting to be retried").register(registry);

        Gauge.builder("job.processing.queue.depth", jedisPool, pool -> {
            try (Jedis jedis = pool.getResource()) {
                return (double) jedis.llen(RedisKeys.PROCESSING_QUEUE);
            } catch (Exception e) {
                return 0.0;
            }
        }).description("Jobs currently being processed").register(registry);

        Gauge.builder("job.dlq.depth", jedisPool, pool -> {
            try (Jedis jedis = pool.getResource()) {
                return (double) jedis.llen(RedisKeys.DEAD_LETTER_QUEUE);
            } catch (Exception e) {
                return 0.0;
            }
        }).description("Jobs in dead letter queue").register(registry);
    }
    

}

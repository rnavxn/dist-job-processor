package com.example.jobqueue.dist_job_processor.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
@Profile("worker")
public class WorkerThreadPoolConfig {

    @Bean(name = "jobTaskExecutor")
    public ThreadPoolTaskExecutor jobTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();

        executor.setCorePoolSize(50);
        executor.setMaxPoolSize(50);
        // Queue size before rejecting new tasks
        executor.setQueueCapacity(0);

        // Give threads a nice name for the logs
        executor.setThreadNamePrefix("DJP-Worker-"); 
        
        // CRITICAL: Tells Spring to wait for jobs to finish before shutting down!
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(30);
        
        executor.initialize();
        return executor;
    }
}
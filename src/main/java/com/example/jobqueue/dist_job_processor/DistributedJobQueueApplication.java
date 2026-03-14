package com.example.jobqueue.dist_job_processor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class DistributedJobQueueApplication {

	public static void main(String[] args) {
		SpringApplication.run(DistributedJobQueueApplication.class, args);
	}

}

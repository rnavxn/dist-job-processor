package com.example.jobqueue.dist_job_processor.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

@Configuration
public class RedisConfig {

    @Value("${redis.host}")
    private String host;

    @Value("${redis.port}")
    private int port;

    @Value("${redis.password}")
    private String password;

    @Bean
    public JedisPool jedisPool() {
        // Upstash requires SSL (true) for secure connections
        return new JedisPool(new JedisPoolConfig(), host, port, 2000, password, true);
    }
}

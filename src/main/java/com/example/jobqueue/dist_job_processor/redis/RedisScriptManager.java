package com.example.jobqueue.dist_job_processor.redis;

import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

@Service
public class RedisScriptManager {

    private final Map<String, String> scripts = new HashMap<>();

    @PostConstruct
    public void loadScripts() {
        scripts.put("retry_to_job", load("lua/retry_to_job.lua"));
        scripts.put("processing_to_retry", load("lua/processing_to_retry.lua"));
        scripts.put("processing_to_dlq", load("lua/processing_to_dlq.lua"));
        scripts.put("processing_to_job", load("lua/processing_to_job.lua"));
    }

    public String get(String name) {
        return scripts.get(name);
    }

    private String load(String path) {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(path)) {

            if (is == null) {
                throw new RuntimeException("Lua script not found: " + path);
            }

            return new String(is.readAllBytes(), StandardCharsets.UTF_8);

        } catch (Exception e) {
            throw new RuntimeException("Failed to load Lua script: " + path, e);
        }
    }
}

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
        scripts.put("retry_move", load("lua/retry_move.lua"));
        scripts.put("fail_to_retry", load("lua/fail_to_retry.lua"));
        scripts.put("fail_to_dlq", load("lua/fail_to_dlq.lua"));
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

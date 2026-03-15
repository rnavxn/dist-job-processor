package com.example.jobqueue.dist_job_processor.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import java.util.Map;
import java.util.HashMap;

@Controller // Note: Not @RestController because we want to serve HTML
@RequiredArgsConstructor
public class DashboardController {

    private final JedisPool jedisPool;

    @GetMapping("/dashboard")
    public String getStatus(Model model) {
        try (Jedis jedis = jedisPool.getResource()) {
            Map<String, Long> stats = new HashMap<>();
            stats.put("waiting", jedis.llen("job_queue"));
            stats.put("processing", jedis.llen("processing_queue"));
            stats.put("failed", jedis.llen("dead_letter_queue"));

            model.addAttribute("stats", stats);
        }
        return "dashboard"; // This looks for dashboard.html in templates folder
    }
}
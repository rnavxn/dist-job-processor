package com.example.jobqueue.dist_job_processor.controller;

import com.example.jobqueue.dist_job_processor.redis.RedisKeys;
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

    /**
     * Simple HTML dashboard for quick queue status.
     *
     * Shows current queue sizes (waiting, processing, failed).
     * Auto-refreshes every 5 seconds via meta tag in dashboard.html.
     *
     * For detailed metrics with historical data and graphs,
     * use Grafana at <a href="http://localhost:3000">http://localhost:3000</a>
     */
    @GetMapping("/dashboard")
    public String getStatus(Model model) {
        try (Jedis jedis = jedisPool.getResource()) {
            Map<String, Long> stats = new HashMap<>();
            stats.put("waiting", jedis.llen(RedisKeys.JOB_QUEUE));
            stats.put("processing", jedis.llen(RedisKeys.PROCESSING_QUEUE));
            stats.put("failed", jedis.llen(RedisKeys.DEAD_LETTER_QUEUE));
            stats.put("retry", jedis.zcard(RedisKeys.RETRY_QUEUE));

            model.addAttribute("stats", stats);
        } catch (Exception e) {
            model.addAttribute("error", "Redis unavailable");
        }

        return "dashboard"; // This looks for dashboard.html in templates folder
    }
}
package com.example.jobqueue.dist_job_processor.model;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import java.util.UUID;

@NoArgsConstructor
@AllArgsConstructor
public class Job {
    private String id = UUID.randomUUID().toString();
    private String type;        // e.g., "EMAIL_SEND", "IMAGE_RESIZE"
    private String payload;     // The actual data (JSON string or simple text)
    private long createdAt = System.currentTimeMillis();

    public String getId() {
        return this.id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }
}

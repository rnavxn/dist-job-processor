# VoltQueue: Distributed Task Processing Engine

VoltQueue is a fault-tolerant, distributed task queue system built using Java 21, Spring Boot, and Redis. It facilitates asynchronous background processing with a focus on data integrity and system resilience.

## Architecture Overview

The system architecture consists of three core components:

* **Producer:** Exposes a REST API to accept task requests and persists them to a Redis-backed global queue.
* **Workers:** Independent consumer nodes that retrieve tasks using atomic Redis operations and execute them via internal thread pools.
* **Reaper (Watchdog):** A maintenance service that monitors the processing lifecycle and reclaims tasks from stalled or crashed worker nodes.

 ---
## Key Engineering Solutions

### Idempotent Job Reclamation
A primary challenge in distributed queues is the "Zombie Task" problem—where a worker dies while holding a job.
* **Problem:** Initial reclamation logic caused exponential job duplication during high-concurrency windows.
* **Solution:** Implemented strict idempotency checks. The Reaper service now validates the successful removal of a task from the 'Processing' state before re-enqueuing. This ensures "At-Least-Once" delivery without the risk of duplicate inflation.

### Atomic State Management
To ensure zero data loss during task handover, the system utilizes atomic Redis operations. By moving tasks between queues in a single operation, the system maintains a consistent state even if a network interruption occurs during the handover.

---
## Deployment

### Environment Configuration
Create a `.env` file in the root directory:
```dotenv
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=your_password
```

### Cluster Initialization
The project utilizes a multi-stage Docker build to compile and package the application. To build and start the Producer and Worker nodes:
```dockerfile
docker-compose up --build
```
---

### API Specification
**Endpoint:** `POST /api/jobs/enqueue`

**Parameters:**
* `type` (String): The category of the task.
* `payload` (String): The data payload for execution.

### Monitoring
A real-time dashboard is available at `http://localhost:8080/dashboard` to monitor:

* **Waiting:** Pending tasks.
* **Processing:** Active tasks across all nodes.
* **Failed:** Tasks moved to the Dead Letter Queue.


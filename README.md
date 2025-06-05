# Data Ingestion API System

## Overview
This project implements a simple Data Ingestion API System with two RESTful endpoints:

- **POST /ingest**: Submit ingestion requests containing a list of IDs and a priority.
- **GET /status/{ingestion_id}**: Retrieve the status of the ingestion request and its batches.

The system processes IDs asynchronously in batches of 3, respects a rate limit of 1 batch per 5 seconds, and prioritizes requests based on priority and creation time.

---

## Features

- **Batch processing**: Max 3 IDs processed concurrently per batch.
- **Asynchronous execution**: Batches are processed asynchronously to optimize throughput.
- **Rate limiting**: Only 1 batch processed every 5 seconds.
- **Priority-based queueing**: Higher priority requests are processed before lower priority ones.
- **Simulated external API**: Each ID is processed with a simulated delay and static response.
- **Status tracking**: Monitor ingestion request and batch statuses (`yet_to_start`, `triggered`, `completed`).

---

## API Endpoints

### POST /ingest

**Request Body:**

```json
{
  "ids": [1, 2, 3, 4, 5],
  "priority": "HIGH"
}
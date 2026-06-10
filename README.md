# FIT3182 — Streaming Speed Camera Pipeline
## Authors
- Student IDs: 34080678 | 33590982

## Overview
This project implements a Spark Structured Streaming pipeline that consumes three Kafka topics (`camera-events-A`, `camera-events-B`, `camera-events-C`), detects instantaneous and average-speed violations, and writes per-vehicle daily violation documents to MongoDB (`fit3182_db.violation`). The streaming application and data-model are implemented in `src/34080678_33590982_data_design_streaming.ipynb`. Visualization work is in `src/34080678_33590982_visualisation.ipynb`.

## Quick Start / Prerequisites
- Docker + Docker Compose (runs Kafka, MongoDB, and a Jupyter/PySpark environment in one stack — see **Running with Docker Compose** below).
- Ensure Kafka producers (`producer_a`, `producer_b`, `producer_c`) are running before starting the streaming notebook.

## Running with Docker Compose
The whole stack — **Kafka** (KRaft mode, no Zookeeper), **MongoDB**, and a **Jupyter/PySpark** environment with the notebooks mounted — is defined in `docker-compose.yml`. All configuration (hosts, ports, topic names, data paths, streaming parameters) is centralised in a single `.env` file that both `docker-compose.yml` and the notebooks read (via `python-dotenv`), so nothing needs to be hand-edited per machine anymore.

1. Copy the example environment file and adjust values if needed:
   ```
   cp .env.example .env
   ```
2. Build and start the stack:
   ```
   docker compose up -d --build
   ```
3. Open Jupyter at `http://<host-ip>:8888` (default port from `.env`'s `JUPYTER_PORT`). On a cloud VM (e.g. Oracle Cloud), use the instance's public/private IP and make sure the VCN security list / NSG allows ingress on that port.
4. Run the notebooks from `src/` in order (see **How to run** below) — they resolve Kafka and MongoDB via the in-network service names `kafka` and `mongodb`.
5. Stop the stack with `docker compose down` (add `-v` to also remove the persisted Kafka/MongoDB/checkpoint volumes).

**Key `.env` variables** (see `.env.example` for the full list and defaults):
| Variable | Purpose |
|---|---|
| `MONGO_HOST` / `MONGO_PORT` / `DB_NAME` | MongoDB connection (defaults to the `mongodb` compose service) |
| `KAFKA_HOST` / `KAFKA_PORT` | Kafka bootstrap server (defaults to the `kafka` compose service) |
| `TOPIC_A` / `TOPIC_B` / `TOPIC_C` | Kafka topic names for each producer |
| `DATA_DIR` | Container-internal data directory (bind-mounted from `./data`) |
| `WATERMARK_DELAY` / `JOIN_WINDOW` / `JOIN_WINDOW_SECONDS` | Streaming state parameters |
| `BATCH_SLEEP_A` / `BATCH_SLEEP_B` / `BATCH_SLEEP_C` | Producer batch cadence (seconds) |
| `JUPYTER_PORT` / `MONGO_EXPOSED_PORT` / `KAFKA_EXPOSED_PORT` | Host-side port mappings |

## How to run
1. Start the stack with Docker Compose (see above) — Kafka and MongoDB come up automatically.
2. Confirm `camera` and `vehicle` collections are created by running the Task 1 cells in `src/34080678_33590982_data_design_streaming.ipynb`.
3. Start Kafka producers (`producer_a`, `producer_b`, `producer_c`) so events begin flowing.
4. Run Task 2 cells to start the Spark streaming job.
5. Stop the stream with a kernel interrupt or programmatic `query.stop()`.
6. To start fresh, optionally clear the checkpoint folder before re-running the stream:
   - Python: `shutil.rmtree("./checkpoints/violations", ignore_errors=True)`

## Deploying to an AWS EC2 Instance
The stack can also be deployed to a cloud VM such as an AWS EC2 instance so the notebooks and Spark UI are reachable over the internet.

1. **Create a free-tier AWS account** and sign in to the EC2 console.
2. **Launch an instance**, making sure to create (or select) a **key pair** — you'll need the `.pem` file to connect over SSH.
3. **Connect to the instance**, either:
   - via the AWS console's **Connect** button (browser-based SSH), or
   - via a terminal using the key pair you saved:
     ```
     ssh -i "/path/to/your-key.pem" ec2-user@<ec2-public-ip>
     ```
4. **Update the system and install Docker:**
   ```
   sudo dnf update -y
   sudo dnf install docker -y
   ```
5. **Start and enable Docker:**
   ```
   sudo systemctl enable docker
   sudo systemctl start docker
   docker --version
   ```
6. **Install the Docker Buildx and Compose CLI plugins:**
   ```
   mkdir -p ~/.docker/cli-plugins/

   curl -SL https://github.com/docker/buildx/releases/download/v0.17.0/buildx-v0.17.0.linux-amd64 \
     -o ~/.docker/cli-plugins/docker-buildx
   chmod +x ~/.docker/cli-plugins/docker-buildx

   curl -SL https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64 \
     -o ~/.docker/cli-plugins/docker-compose
   chmod +x ~/.docker/cli-plugins/docker-compose

   docker buildx version
   ```
7. **Add your user to the `docker` group** so you don't need `sudo` for every Docker command:
   ```
   sudo usermod -aG docker $USER
   ```
8. **Install Git and clone the repository:**
   ```
   sudo dnf install git -y
   git --version

   git clone https://github.com/rvoo0001/FIT3182.git
   cd FIT3182
   git pull
   ```
9. **Create your `.env` file from the example** and verify it:
   ```
   cp .env.example .env
   ls -la
   cat .env
   ```
10. **Build and start the stack:**
    ```
    docker compose up -d --build
    ```
11. **Access the running services:**
    - Jupyter Notebook: http://43.216.253.136:8888 (token: `0bec4c3fd4c4f0b7fa9bfeeb5a7124368738ad1c97ba7d8f` — stays the same every time)
    - Spark UI: http://43.216.253.136:4040 (only reachable once the streaming application is running)

    > Make sure the EC2 **security group** allows inbound traffic on these ports (8888, 4040, and any others from `.env`) from your IP or `0.0.0.0/0` for testing.

## Important design notes
- Join semantics: average-speed detection uses time-bounded inner joins on `car_plate` with an ordering constraint (`a.event_time < b.event_time`) and `b.event_time <= a.event_time + interval JOIN_WINDOW`. `batch_id` is not used for joins.
- Sink semantics: `foreachBatch` uses `update_one(..., upsert=True)` with `$push` to append violation events into a daily document keyed by `(car_plate, date)`. Top-level document duplication is prevented by a compound unique index, but event-level duplicates (e.g. on Spark retry) are possible unless you add de-duplication logic (e.g. use `event_id` and check before `$push`).
- Tuning `JOIN_WINDOW`: default `60 seconds` captures vehicles averaging ≥60 km/h over the 1 km segment (good for stricter, low-noise detection). Increasing to `300s` (5 min) captures slower vehicles but increases state and false matches.

## Performance & Scalability Testing
`src/34080678_33590982_data_design_streaming.ipynb` includes a self-contained scalability/throughput/latency experiment (Task 2 cells) used to evaluate the pipeline against the performance-analysis criteria.

### How it works
- Set `RUN_SCALABILITY_TEST = True` (default `False`) to enable the experiment — when `False`, the notebook behaves exactly like the normal workflow and the cell is a no-op.
- The test runs the **full pipeline three times**, once per entry in `SCALABILITY_BATCH_LIMITS = [10, 20, 40]` (doubling input volume each run). For each run it:
  1. Stops any active Spark queries and clears the checkpoint (`./checkpoints/violations`) for a fresh Kafka offset subscription.
  2. Optionally clears the `violation` collection (`CLEAR_OUTPUT_BEFORE_EACH_RUN`) so each run's results are isolated.
  3. Starts a fresh streaming query (instantaneous detection + A→B and B→C average-speed joins).
  4. Runs producers A, B, and C in background threads, restricted to `batch_id` 1..N.
  5. Waits for the producers to finish, then a `DRAIN_BUFFER_SECS` (180s) buffer so in-flight micro-batches and watermark-bound joins can complete and flush to MongoDB.
  6. Stops the query and records metrics for that run.

### Metrics captured (per run)
| Metric | Definition |
|---|---|
| `input_records` | Total camera events sent across all three producers |
| `processing_time_s` | Wall-clock time for the run (start → end of drain buffer) |
| `throughput_rec_s` | `input_records / processing_time_s` |
| `violation_count` | Violations written to MongoDB during the run |
| `avg/min/max_latency_ms` | End-to-end latency per violation: `written_at` (MongoDB sink) − `produced_at` (Kafka producer timestamp) |

Results are saved to `src/performance_results.csv` and displayed as a summary table by the following cell, ready to copy into a report or slides.

### Findings
The test was run on both a local machine (Windows + Docker Desktop) and the deployed AWS EC2 instance:

| Input Records | AWS throughput (rec/s) | Local throughput (rec/s) | AWS avg latency | Local avg latency |
|---|---|---|---|---|
| 240 | 1.02 | 1.10 | 64.5s | 163.5s |
| 467 | 1.65 | 1.70 | 51.5s | 132.8s |
| 930 | 2.42 | 2.60 | 47.4s | 128.4s |

- **Scalability**: violation counts scale roughly linearly with input volume on both environments, confirming the join/detection logic is `O(n)`.
- **Throughput**: comparable between AWS and local — bounded by the controlled producer cadence and drain buffer, not infrastructure.
- **Latency**: AWS is consistently 2.5–2.7x lower than local. This is caused by Spark's per-micro-batch checkpoint commits (`./checkpoints/violations`), which live on the Docker bind-mounted `./src` directory. On AWS (native Linux Docker), these writes are near-native; on Windows (Docker Desktop/WSL2), each write crosses a translation layer to the host NTFS filesystem, slowing the micro-batch trigger loop and increasing end-to-end latency. MongoDB write performance is not a factor, since its data lives in a Docker named volume on both environments.

See the "Performance Evaluation — Discussion" markdown cell in the notebook for the full write-up.

## Generative AI Usage Declaration

GitHub Copilot (Claude Sonnet 4.6) was used to assist with implementation, debugging, and documentation preparation.
The prompts included requests for guidance on implementing the Spark Structured Streaming pipeline, designing the MongoDB collection schema and sink pattern, implementing the three Kafka producer notebooks, and developing the visualisation charts. In addition, AI was used to support the drafting of notebook documentation cells and the README by generating technical explanations based on the implemented code.
All generated outputs were carefully reviewed, modified, and validated. The final submitted code and documentation reflect our own understanding and have been tested to ensure correctness and compliance with the assignment requirements.

**Representative prompts used:**
- *"Design a MongoDB schema for a daily violation document using an embedded array and `$push` upsert pattern."*
- *"Implement three Kafka producer notebooks that read camera event CSVs, group rows by `batch_id`, and publish to separate Kafka topics with a scaled sleep interval."*
- *"Implement a Spark Structured Streaming pipeline with a time-bounded stream-stream join on `car_plate` to detect average-speed violations, and write results to MongoDB using `foreachBatch`."*
- *"Generate four visualisation chart functions with consistent styling and arrow-callout annotations for peak, trough, and notable speed events."*
- *"Draft documentation cells explaining the watermark delay, join window justification, and sleep time scaling formula for each producer."*



# FIT3182 — Streaming Speed Camera Pipeline
## Authors
- Student IDs: 34080678 | 33590982

## Overview
This project implements a Spark Structured Streaming pipeline that consumes three Kafka topics (`camera-events-A`, `camera-events-B`, `camera-events-C`), detects instantaneous and average-speed violations, and writes per-vehicle daily violation documents to MongoDB (`fit3182_db.violation`). The streaming application and data-model are implemented in `src/34080678_33590982_data_design_streaming.ipynb`. Visualization work is in `src/34080678_33590982_visualisation.ipynb`.

## Quick Start / Prerequisites
- Docker running locally (MongoDB and Kafka producers run in containers).
- A PySpark environment (the notebook sets `PYSPARK_SUBMIT_ARGS` for Kafka packages).
- Python 3.x, `pandas`, `pymongo`, `pyspark` (as used in notebooks).
- Ensure Kafka producers (`producer_a`, `producer_b`, `producer_c`) are running before starting the streaming notebook.

## Configuration (what to edit)
- MongoDB IP: run on host machine:
  - `docker inspect "container_name" --format "{{.NetworkSettings.IPAddress}}"`
  - Set `MONGO_HOST` and `MONGO_PORT` in `src/34080678_33590982_data_design_streaming.ipynb`.
- Producers host IP (Windows): run `ipconfig` → copy the IPv4 address → set `HOST_IP` in each producer script.
- Data directory: update `DATA_DIR` in notebooks if your project paths differ.
- Join window and watermark defaults are in the notebook:
  - `WATERMARK_DELAY = "10 minutes"`
  - `JOIN_WINDOW = "60 seconds"` (tune as needed; see notes below)
- To start fresh, optionally clear checkpoint folder before running the stream:
  - Python: `shutil.rmtree("./checkpoints/violations", ignore_errors=True)`

## How to run
1. Start MongoDB and Kafka producers (Docker).
2. Confirm `camera` and `vehicle` collections are created by running the Task 1 cells in `src/34080678_33590982_data_design_streaming.ipynb`.
3. Run Task 2 cells to start the Spark streaming job (ensure producers are publishing).
4. Stop the stream with a kernel interrupt or programmatic `query.stop()`.

## Important design notes
- Join semantics: average-speed detection uses time-bounded inner joins on `car_plate` with an ordering constraint (`a.event_time < b.event_time`) and `b.event_time <= a.event_time + interval JOIN_WINDOW`. `batch_id` is not used for joins.
- Sink semantics: `foreachBatch` uses `update_one(..., upsert=True)` with `$push` to append violation events into a daily document keyed by `(car_plate, date)`. Top-level document duplication is prevented by a compound unique index, but event-level duplicates (e.g. on Spark retry) are possible unless you add de-duplication logic (e.g. use `event_id` and check before `$push`).
- Tuning `JOIN_WINDOW`: default `60 seconds` captures vehicles averaging ≥60 km/h over the 1 km segment (good for stricter, low-noise detection). Increasing to `300s` (5 min) captures slower vehicles but increases state and false matches.

## AI / Generative Assistance Declaration
Generative AI was used in parts of this work. Declaration:
- Tools used: Claude Sonnet 4.6; additional assistance from a conversational AI for analysis and wording during development.
- Purpose: data analysis, diagram creation, structural understanding of the streaming pipeline, debugging dependency and Docker setup, and improving documentation language.
- Prompt summary / examples: Explain streaming join semantics (time-bounded joins vs batch offsets), justify `JOIN_WINDOW` choices, convert ASCII architecture diagrams to prose, walkthrough detection logic (instantaneous vs average), and prepare README/documentation text. Also used AI to generate visualization code in `src/34080678_33590982_visualisation.ipynb`.



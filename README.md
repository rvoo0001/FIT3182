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

## Generative AI Usage Declaration

Generative AI was used throughout this assignment. All AI-generated output was reviewed, tested, and validated before submission.

- **Tool:** GitHub Copilot (Claude Sonnet 4.6)
- **Scope:** Entire assignment — implementation and documentation for all tasks
- **Review:** All generated code and text was read, tested, and corrected by the authors

### Purpose by Task

- **Task 1 (MongoDB Data Model):** Drafting collection schemas, sample documents, index definitions, shard-key rationale, retention policies, and the embed-vs-reference justification.
- **Task 2.1.1 (Kafka Producers):** Implementing batch-based Kafka producer notebooks that read from CSV files and publish events keyed by `batch_id`.
- **Task 2.1.2 (Streaming Join Logic):** Designing and implementing the Spark Structured Streaming time-bounded inner join, watermark configuration, and stream-stream join with `car_plate` ordering constraints.
- **Task 2.1.3 (MongoDB Sink):** Writing the `foreachBatch` sink using `update_one` with `$push` and upsert semantics; compound unique index design for idempotency.
- **Task 2.1.4 (Violation Detection):** Implementing instantaneous and average-speed violation rules, per-vehicle daily document merging, and `avg_speed` calculation from segment distance and travel time.
- **Task 2.2.1 (Data Visualisation):** Generating all four chart functions (`plot_violation_count`, `plot_speed_over_time`, `plot_distribution`, `plot_stacked_composition`) with consistent styling, labels, and static PNG exports.
- **Task 2.2.2 (Interesting Point Annotation):** Adding peak, trough, surge, max/min speed, and dominant-camera/segment annotations with arrow callouts; writing the operational summary table.
- **Task 3 (Documentation & Code Quality):** Writing docstrings, inline comments, markdown narrative cells, the README, and ensuring consistent code style across all notebooks.

### Prompt Summary

The following is a representative summary of the prompts used. Prompts were issued iteratively; each was reviewed and the output refined before use.

- *"Design a MongoDB schema for a daily violation document that groups multiple violation events per vehicle per day. Include embedded subdocuments, a compound unique index, and a `$push` upsert pattern for the Spark sink."*
- *"Implement three Kafka producer notebooks that read `camera_event_A/B/C.csv`, group rows by `batch_id`, and publish each batch to a separate Kafka topic every N seconds. Include producer metadata fields for source identification."*
- *"Implement a Spark Structured Streaming application with a time-bounded stream-stream inner join on `car_plate`. Detect instantaneous violations (speed > camera limit) and average-speed violations (avg speed over 1 km segment > ending camera limit). Write violations to MongoDB using `foreachBatch`."*
- *"Move the `shutil.rmtree` checkpoint-clearing code out of the `foreachBatch` sink function into its own standalone notebook cell so it only runs on explicit fresh-start, not on every micro-batch."*
- *"Refactor the visualisation notebook to produce four charts: violation count over time, average violation speed over time, distribution by camera and road segment, and stacked type composition. Use consistent colour constants, `rcParams`, and a shared `_ann()` helper. Add arrow-callout annotations for peak, trough, surge, max speed, min speed, speed drop, dominant camera, dominant segment, and highest average-speed share."*
- *"Replace `clear_output(wait=True)` in the live monitoring loop with `IPython.display.update_display()` using named `display_id` slots so charts update in-place without a blank-screen flash between refreshes."*
- *"Write docstrings for all chart functions, add a summary operational insights table as a markdown cell, and produce a printed operational summary cell that reports peak period, violation counts, speed stats, busiest camera, and busiest segment."*
- *"Review all markdown documentation cells for consistency with the implemented code — check data source descriptions, shading descriptions, annotation descriptions, and remove any references to the removed CSV fallback."*



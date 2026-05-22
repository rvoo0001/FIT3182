# Task 1 — MongoDB Data Model Explanation

**Student IDs:** 34080678 | 33590982

---

## Overview

Task 1 designs and creates the MongoDB database for the Automatic Web Averaging System (AWAS) traffic speed camera system. The database is named `fit3182_db` and contains three collections that together support both historical record-keeping and the real-time streaming pipeline built in Task 2.

---

## Task 1.1 — Collection Design

Three collections are created:

| Collection | Purpose |
|---|---|
| `vehicle` | Registered vehicle and owner information |
| `camera` | Speed camera locations and speed limits |
| `violation` | Detected speeding violations (populated by the Task 2 streaming pipeline) |

Each collection is created with:
- A **JSON Schema validator** that enforces required fields and data types at the database level
- **Indexes** chosen to support the expected read/write patterns
- A defined **shard key** for horizontal scalability
- A **retention policy** appropriate to the data's legal or operational lifetime

---

### Collection 1: `vehicle`

**Purpose:** Stores the registered vehicle records loaded from `vehicle.csv`. Each document represents one vehicle and its registered owner.

**Document structure:**

| Field | Type | Description |
|---|---|---|
| `car_plate` | string | Unique licence plate — primary key |
| `owner_name` | string | Full name of the registered owner |
| `owner_addr` | string | Residential address |
| `vechicle_type` | string | One of: Sedan, SUV, Coupe, Hatchback, Van, Truck |
| `registration_date` | Date | ISO 8601 date of registration |

**Sample document:**
```json
{
  "car_plate": "ABC123",
  "owner_name": "Jane Smith",
  "owner_addr": "12 Example St, Melbourne VIC 3000",
  "vechicle_type": "Sedan",
  "registration_date": "2024-01-01T00:00:00Z"
}
```

**Indexes:**

| Index | Type | Reason |
|---|---|---|
| `car_plate` | Unique ascending | Primary lookup key; enforces uniqueness across all vehicles |
| `vechicle_type` | Ascending | Supports analytics queries filtered by vehicle category |

**Shard key:** `car_plate` — high cardinality, evenly distributed, and matches the join key used in the `violation` collection.

**Retention policy:** Records are kept indefinitely. Vehicles deregistered for more than 7 years may be archived to cold storage.

**Data loading notes:**
- Loaded from `vehicle.csv`
- Duplicates on `car_plate` are dropped (first occurrence kept) before insertion
- `registration_date` is parsed from string to a proper Python `datetime` object so MongoDB stores it as a BSON `date` type

---

### Collection 2: `camera`

**Purpose:** Stores speed camera metadata including GPS coordinates, road position, and the enforced speed limit. This is a small, read-heavy, rarely updated reference collection. It is referenced by both the streaming event data and the `violation` collection to avoid duplicating speed-limit values.

**Document structure:**

| Field | Type | Description |
|---|---|---|
| `camera_id` | int | Unique camera identifier — primary key |
| `latitude` | double | GPS latitude |
| `longitude` | double | GPS longitude |
| `position` | double | Road position marker (km along the road) |
| `speed_limit` | int | Enforced speed limit at this camera (km/h) |

**Sample document:**
```json
{
  "camera_id": 1,
  "latitude": -37.8136,
  "longitude": 144.9631,
  "position": 0.0,
  "speed_limit": 100
}
```

**Indexes:**

| Index | Type | Reason |
|---|---|---|
| `camera_id` | Unique ascending | Primary lookup; used in joins from event and violation records |
| `speed_limit` | Ascending | Supports queries filtering cameras by speed zone |

**Shard key:** `camera_id` — monotonically increasing integer, suitable for range-based sharding.

**Retention policy:** Permanent. Camera records represent physical infrastructure. Decommissioned cameras should be marked inactive rather than deleted, to preserve the context of historical violations recorded by that camera.

---

### Collection 3: `violation`

**Purpose:** Stores confirmed speeding violations detected by the real-time Spark Structured Streaming pipeline (Task 2). This collection is **intentionally left empty** at Task 1 setup — the streaming consumer will write to it.

**Design choice — one document per vehicle per day:**  
Rather than creating one document per individual violation event, the design groups all violations for a given vehicle on a given calendar date into a **single document with an embedded array**. This minimises document count and aligns with the streaming upsert pattern used in Task 2 (`$push` into the array).

**Document structure:**

| Field | Type | Description |
|---|---|---|
| `car_plate` | string | Reference to `vehicle.car_plate` |
| `date` | Date | Calendar date of violations (midnight UTC) |
| `violations` | array | Embedded array of violation events for that day |

Each element of the `violations` array:

| Sub-field | Type | Description |
|---|---|---|
| `violation_type` | string | `"INSTANTANEOUS"` or `"AVERAGE"` |
| `camera_id_start` | int | Entry camera (equals `camera_id_end` for instantaneous) |
| `camera_id_end` | int | Exit camera |
| `timestamp_start` | Date | Event time at entry camera |
| `timestamp_end` | Date | Event time at exit camera |
| `speed_reading` | double | Recorded or computed speed (km/h) |

**Sample document:**
```json
{
  "car_plate": "ABC123",
  "date": "2024-01-01T00:00:00Z",
  "violations": [
    {
      "violation_type": "INSTANTANEOUS",
      "camera_id_start": 1,
      "camera_id_end": 1,
      "timestamp_start": "2024-01-01T08:00:04Z",
      "timestamp_end": "2024-01-01T08:00:04Z",
      "speed_reading": 125.0
    },
    {
      "violation_type": "AVERAGE",
      "camera_id_start": 1,
      "camera_id_end": 2,
      "timestamp_start": "2024-01-01T08:00:00Z",
      "timestamp_end": "2024-01-01T08:00:33Z",
      "speed_reading": 118.2
    }
  ]
}
```

**Indexes:**

| Index | Type | Reason |
|---|---|---|
| `(car_plate, date)` | Compound unique ascending | Primary upsert key; ensures exactly one document per vehicle per day |
| `date` | Ascending | Supports time-range queries across all vehicles |

**Shard key:** `car_plate` — consistent with the `vehicle` collection; co-locates a vehicle's documents across shards.

**Retention policy:** Violations are legally significant records and are retained indefinitely. A TTL index on an `archived_at` field (e.g. 7 years) could be applied for regulatory purging if required.

---

## Task 1.2 — Collection Relationships

### Relationship Diagram

```
vehicle (car_plate  ← PK)
    │
    └──── referenced by ──── violation.car_plate

camera (camera_id  ← PK)
    │
    ├──── referenced by ──── violation.violations[].camera_id_start
    └──── referenced by ──── violation.violations[].camera_id_end
```

The `vehicle` and `camera` collections act as **reference/lookup** tables. The `violation` collection is the primary write target of the streaming pipeline and holds references (not embedded copies) to both.

---

### Design Decisions: Embed vs. Reference

#### `violation` → `vehicle` (via `car_plate`) — **Reference**

- Violations are written once and read many times (reports, dashboards, enforcement queries).
- Owner details can change over time (address updates, ownership transfers). Storing a reference means queries always return the current owner information without needing to backfill existing violation documents.
- Trade-off: a query that needs owner details requires a two-collection lookup (`violation` → `vehicle`). This is acceptable because violation queries are typically filtered first by `car_plate` or `date`, and the vehicle lookup is a fast point query on a unique index.

#### `violation` → `camera` (via `camera_id_start` / `camera_id_end`) — **Reference**

- There are only 3 cameras — the `camera` collection is tiny and fits entirely in memory.
- Referencing avoids duplicating speed-limit and location values across every violation sub-document.
- Camera metadata is stable reference data; the join cost at query time is negligible.

#### Individual violation events inside `violation` — **Embed**

- Each `violation` document groups all events for a `car_plate` + `date` pair. Individual violation events are **embedded** as an array rather than stored as separate top-level documents.
- This aligns with the Task 2 streaming upsert pattern: the Spark consumer uses a single `update_one(..., upsert=True)` with `$push` per batch, avoiding expensive per-event document creation.
- Trade-off: if a vehicle accumulates many violations in a single day the array could grow large. In practice, daily violations per vehicle are rare, so this is not a concern.

---

### Consistency Considerations

MongoDB does not enforce foreign-key constraints. Application-level consistency is maintained by:

1. **Insert order** — `vehicle` and `camera` documents are loaded before the streaming pipeline starts writing `violation` documents.
2. **Validation schemas** — each collection's JSON Schema validator rejects documents with missing required fields or invalid types at write time.
3. **Unique compound index** on `(car_plate, date)` — prevents duplicate daily documents from stream retries, making upserts idempotent.

---

### Summary Table

| Relationship | Strategy | Reason |
|---|---|---|
| `violation.car_plate` → `vehicle` | Reference | Owner data changes; reference ensures freshness |
| `violation.violations[].camera_id_*` → `camera` | Reference | Camera list is tiny, static reference data |
| Individual violation events inside `violation` | Embed | Groups daily events for efficient `$push` upsert; low growth risk |

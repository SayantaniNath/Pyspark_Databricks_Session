# PySpark & Databricks — Full Learning Path
## FAANG / Top-Tier Data Engineer Interview Curriculum

> Format: **Teach → Exercises → Next-day quick revision**
> Each session: 1 concept at a time, full explanation, from-blank exercises after.
> Status: ✅ Done | 🟡 Partial | ⏳ Pending | ⭐ Critical for interviews

---

## MODULE 1 — Spark Architecture & Mental Model

### 1.1 Core Architecture ✅
- Why Spark exists — scale-out vs scale-up
- Driver, Executors, Cluster Manager — roles and communication
- SparkSession as entry point
- Distributed + lazy mental model

### 1.2 Lazy Evaluation ✅
- Transformations vs Actions
- Why lazy eval exists — optimization window
- Narrow vs Wide transformations
- When execution actually triggers

### 1.3 DAG, Jobs, Stages, Tasks ✅
- Job → Stage → Task hierarchy
- Stage boundary = shuffle boundary
- Stage counting recipe
- Partitions are per-stage, not summed

### 1.4 Catalyst Optimizer 🟡
- 4 phases: Parse → Analyze → Optimize → Physical plan
- Predicate pushdown, column pruning, constant folding
- Cost-based join selection
- ⏳ MISSING: ANALYZE TABLE, explain() deep-dive, reading WSCG output

### 1.5 Tungsten Execution Engine 🟡
- Off-heap memory management
- Cache-aware columnar processing
- Whole-stage code generation (WSCG)
- ⏳ MISSING: Reading * notation in explain() output

---

## MODULE 2 — Core DataFrame API

### 2.1 Foundational Operations ✅
- spark.read.json/csv/parquet/delta
- select, filter, withColumn, groupBy, agg, orderBy
- show(), display(), printSchema(), count()
- createOrReplaceTempView + spark.sql()
- explain(True) — reading physical plans

### 2.2 String Functions ⏳
- `upper`, `lower`, `length`, `substring`, `trim`
- `split`, `concat`, `concat_ws`
- `regexp_replace`, `regexp_extract`
- `lpad`, `format_string`
- Practice: Easy 11

### 2.3 Datetime Functions ⏳
- `year()`, `month()`, `dayofweek()`, `hour()`
- `datediff()`, `date_add()`, `date_format()`
- `unix_timestamp()`, `from_unixtime()`
- `add_months()`, `last_day()`, timezone conversion
- Practice: Easy 12, Medium 19

### 2.4 Null Handling ⏳ (PySpark)
- `isNull()`, `isNotNull()`
- `F.coalesce()` — return first non-null
- `fillna()`, `dropna()`
- `when(condition, value).otherwise(value)`
- Practice: Easy 13

### 2.5 Column Operations ⏳
- `cast()` — type conversion
- `when().otherwise()` — conditional logic
- `lit()` — literal values
- `expr()` — SQL expressions in DataFrame API
- Practice: Easy 14

### 2.6 Set Operations ⏳
- `union()` — stack rows (keeps duplicates)
- `unionByName()` — union by column name
- `intersect()` — common rows only
- `subtract()` — rows in A not in B
- `dropDuplicates()` / `distinct()`
- Practice: Easy 15

### 2.7 All Join Types ⏳
- inner, left, right, full outer — covered
- `semi` — return left rows that have a match in right
- `anti` — return left rows that have NO match in right
- `cross` — cartesian product
- Practice: Medium 14

### 2.8 Window Functions (PySpark) ⏳ ⭐
- `Window.partitionBy().orderBy()`
- `F.row_number()`, `F.rank()`, `F.dense_rank()`
- `F.ntile()`, `F.percent_rank()`
- `F.lag()`, `F.lead()`
- `F.first()`, `F.last()`
- Running totals: `F.sum().over(window)`
- Frame spec: `rowsBetween(Window.unboundedPreceding, Window.currentRow)`
- Practice: Medium 01, Medium 17

### 2.9 Array & Complex Types ⏳
- `F.explode()`, `F.explode_outer()`
- `F.collect_list()`, `F.collect_set()`
- `F.array_contains()`, `F.array_distinct()`, `F.flatten()`
- `F.array()`, `create_map()`, `struct()`
- `F.from_json()`, `F.to_json()`
- `posexplode_outer`, `map_entries`, `str_to_map`
- Practice: Medium 12, Medium 15, Medium 21

### 2.10 Schema Management ⏳
- `StructType` / `StructField` for explicit schemas
- Schema inference vs explicit (performance implications)
- Schema evolution with `mergeSchema`
- `printSchema()` reading
- Practice: Hard 10

### 2.11 Higher-Order Functions ⏳
- `transform()` — apply lambda to array
- `filter()` — filter array elements
- `aggregate()` — reduce array to single value
- `zip_with()` — combine two arrays element-wise
- Practice: Medium 18

### 2.12 Pivot & Unpivot ⏳
- `pivot()` — rows to columns
- `stack()` — columns to rows
- `crosstab()` — frequency table
- Practice: Medium 13

### 2.13 Statistical Aggregations ⏳
- `stddev()`, `variance()`, `corr()`
- `percentile_approx()`
- `rollup()`, `cube()` — hierarchical aggregations
- Practice: Medium 20

### 2.14 UDFs & Pandas UDFs ⏳ ⭐
- `@udf` decorator — Python UDF, row-by-row (slow)
- Why Python UDFs are slow — serialization overhead
- `@pandas_udf` — vectorized, Arrow-based (fast)
- `applyInPandas()` — grouped Pandas operations
- When to use UDFs vs native Spark functions
- Practice: Hard 09

---

## MODULE 3 — Partitioning & Shuffles

### 3.1 Partitions ✅
- Partition = unit of parallelism
- How partitions set at read time (maxPartitionBytes = 128MB)
- spark.sql.shuffle.partitions (default 200)
- Checking: `df.rdd.getNumPartitions()`

### 3.2 repartition vs coalesce ✅
- `repartition(n)` — full shuffle, increase or decrease
- `coalesce(n)` — no shuffle, decrease only
- `repartition(n, col)` — partition by column
- Rule: increasing/fixing skew → repartition; writing output → coalesce

### 3.3 Shuffles ✅
- Map side → network transfer → reduce side
- Expensive: disk I/O + network I/O + stage barrier
- What triggers: groupBy, join, orderBy, distinct

### 3.4 Bucketing ⏳ ⭐
- Pre-partition tables by join key at write time
- Eliminates shuffle on joins — both tables bucketed on same key
- `saveAsTable` with `bucketBy` + `sortBy`
- When bucketing beats repartition

### 3.5 Dynamic Partition Pruning (DPP) ⏳
- Runtime pruning based on join filter
- Broadcasts filter from one join side to prune the other
- Works on partitioned tables only — transparent, no code changes

---

## MODULE 4 — Join Strategies

### 4.1 Broadcast Hash Join (BHJ) ✅
- Small table broadcast to every executor — no shuffle
- Default threshold: 10MB
- Force with `F.broadcast()`
- 1 stage only

### 4.2 Sort-Merge Join (SMJ) ✅
- Both sides shuffled + sorted on join key
- 3+ stages — default for large-large joins

### 4.3 Shuffle Hash Join (SHJ) ✅
- One side into hash map, other streams through
- Middle ground

### 4.4 Join Hints & SQL Syntax ⏳
- SQL hint: `/*+ BROADCAST(t) */`
- `MERGE` hint, `SHUFFLE_HASH` hint
- How Spark selects strategy from statistics
- Storage Partition Join (SPJ) for bucketed tables

---

## MODULE 5 — Performance Tuning

### 5.1 AQE ✅
- Coalescing small partitions, switching join strategy, skew handling
- On by default in Spark 3+

### 5.2 Caching ✅
- `cache()` = `persist(MEMORY_AND_DISK)` — lazy
- Storage levels, when to cache vs not, always unpersist

### 5.3 Salting ✅
- Join salting: salt large table + explode small table
- groupBy salting: two-pass aggregation
- Bucket sizing: Max / 200MB or Max / Median ratio
- Detect via Spark UI Summary Metrics (Max vs Median)

### 5.4 Memory Management ⏳ ⭐
- Heap vs off-heap memory
- Storage vs execution memory (unified memory model)
- Memory spill to disk — when and how to fix
- GC overhead diagnosis
- OOM errors — root causes and fixes

### 5.5 UDF Performance ⏳
- Python UDF serialization cost
- Pandas UDF (vectorized) vs Python UDF
- Rewriting UDFs as native Spark functions

---

## MODULE 6 — Spark UI

### 6.1 Spark UI Hands-On ⏳ ⭐
- Jobs / Stages / Tasks tabs
- Summary Metrics: Min / 25th / Median / 75th / Max
- Shuffle read/write bytes — diagnose shuffle-heavy plans
- Task duration distribution — spot stragglers
- GC time percentage
- SQL tab — DAG visualization
- Reading `explain()` output fully (Catalyst plans)
- Identifying spill in Spark UI

---

## MODULE 7 — Delta Lake ⭐

### 7.1 Delta Lake Fundamentals ⏳ ⭐
- ACID on top of Parquet files
- Transaction log (`_delta_log`) — every write recorded as JSON
- Optimistic concurrency control
- Append-only architecture

### 7.2 Time Travel ⏳ ⭐
- `VERSION AS OF n`
- `TIMESTAMP AS OF '2024-01-01'`
- `RESTORE TABLE` to previous version
- Retention period + VACUUM interaction

### 7.3 MERGE (Upsert) ⏳ ⭐
- `MERGE INTO` syntax — matched/not matched
- Upsert + delete patterns
- Idempotent MERGE for exactly-once
- Performance considerations

### 7.4 OPTIMIZE + Z-ORDER ⏳ ⭐
- OPTIMIZE — compact small files (bin packing)
- Why small files accumulate
- Z-ORDER — space-filling curve for data locality
- Data skipping enabled by Z-ORDER
- Multi-column Z-ORDER trade-offs

### 7.5 VACUUM ⏳
- Removes old files not in transaction log
- Default retention: 7 days
- Trade-off: VACUUM removes time travel history

### 7.6 Schema Evolution ⏳
- `mergeSchema` — additive column changes
- Type widening (int → long)
- Schema enforcement — rejects mismatched writes

---

## MODULE 8 — Structured Streaming ⭐

### 8.1 Streaming Fundamentals ⏳ ⭐
- Micro-batch model
- Trigger modes: default, fixed interval, once, continuous
- Output modes: Append, Update, Complete
- Sources: Kafka, file, Delta | Sinks: Kafka, Delta, foreachBatch

### 8.2 Watermarks ⏳ ⭐
- Late data threshold
- `withWatermark(eventTime, "10 minutes")`
- How watermarks finalize windows and clean state

### 8.3 Exactly-Once Semantics ⏳ ⭐
- At-most-once vs at-least-once vs exactly-once
- Replayable sources + idempotent sinks
- Checkpointing for fault tolerance

### 8.4 Checkpoints ⏳
- What's stored (offsets + state)
- Unique checkpoint per query
- Recovery on restart

### 8.5 Stateful Operations ⏳
- Stateful aggregations (groupBy + window)
- Deduplication with state stores
- State size management

---

## MODULE 9 — Delta Live Tables (DLT) ⭐

### 9.1 DLT Fundamentals ⏳ ⭐
- `@dlt.table`, `@dlt.view` decorators
- Materialized views vs Streaming tables vs Views
- Automatic dependency resolution and DAG
- Incremental vs full refresh

### 9.2 Expectations ⏳ ⭐
- `@dlt.expect` — log failures, continue
- `@dlt.expect_or_drop` — drop bad rows
- `@dlt.expect_or_fail` — halt pipeline
- Metrics: passed/failed/dropped counts

### 9.3 Pipeline Management ⏳
- Development vs Production mode
- Auto-scaling, retries, monitoring

---

## MODULE 10 — Unity Catalog ⭐

### 10.1 Architecture ⏳ ⭐
- Three-level namespace: `catalog.schema.table`
- Metastore as top-level governance unit
- Securable objects: tables, views, volumes, UDFs, models

### 10.2 Access Control ⏳
- `GRANT` / `REVOKE` syntax
- Catalog → Schema → Table permission hierarchy
- Row-level and column-level security

### 10.3 Lineage & Governance ⏳
- Automatic data lineage tracking
- Column-level lineage
- Tags and classifications (PII, PHI)
- Audit logging

---

## MODULE 11 — Data Modeling ⭐

### 11.1 Star Schema ⏳ ⭐
- Fact tables — transactions, grain definition
- Dimension tables — context, attributes
- Star vs snowflake schema
- Conformed dimensions

### 11.2 Slowly Changing Dimensions (SCDs) ⏳ ⭐
- Type 1 — Overwrite
- Type 2 — New row with effective_from / effective_to
- Type 3 — Historical column
- SCD Type 2 with Delta MERGE

### 11.3 Medallion Architecture ⏳ ⭐
- Bronze → Silver → Gold layers
- Bronze: raw ingestion
- Silver: cleaned, deduplicated, typed
- Gold: aggregated, star schema, business-ready

---

## MODULE 12 — Databricks-Specific

### 12.1 Auto Loader ⏳ ⭐
- Incremental file ingestion from cloud storage
- Automatic schema inference and evolution
- File notification vs directory listing mode
- Bronze layer ingestion pattern

### 12.2 Photon Engine ⏳
- Native C++ vectorized query engine
- 2–8× speedups, 12× on benchmarks
- When Photon helps vs doesn't
- DBU pricing implications

### 12.3 Databricks Workflows ⏳
- Multi-task job orchestration
- Task dependencies and DAG
- Job clusters vs all-purpose clusters

---

## MODULE 13 — Open Lakehouse Formats

### 13.1 Delta vs Iceberg vs Hudi 🟡
- Overview + comparison table: in PySpark_QA_Log.html ✅
- ⏳ MISSING: Iceberg hidden partitioning internals
- ⏳ MISSING: Hudi COW vs MOR storage types
- ⏳ MISSING: Apache Iceberg table API with Spark

---

## MODULE 14 — Temporal Analytics (Hard)

### 14.1 Temporal Patterns ⏳
- First vs repeat purchase detection
- Inter-purchase gap analysis
- Lapsed vs active customer segmentation
- Week-over-week revenue momentum
- Cohort retention rate
- Consecutive purchase streaks (gaps-and-islands)
- Practice: Hard 19

---

## Practice Sessions Map

| When topic is taught | Add these exercises |
|---|---|
| String functions | Easy 11 |
| Datetime functions | Easy 12, Medium 19 |
| Null handling (PySpark) | Easy 13 |
| Column ops (when/otherwise) | Easy 14 |
| Set operations | Easy 15 |
| Window functions | Medium 01, Medium 17 |
| Array/complex types | Medium 12, Medium 15 |
| All join types | Medium 14 |
| Pivot/Unpivot | Medium 13 |
| Statistical aggs | Medium 20 |
| UDFs | Hard 09 |
| Delta Lake | Hard 14 |
| Performance tuning | Hard 11 |
| Temporal analytics | Hard 19 |

---

## Revised Timeline

| Module | Target |
|---|---|
| 2.2–2.6 (string, datetime, nulls, when, set ops) | Mon Jun 9 (gap-fill session) |
| 7 — Delta Lake | Mon Jun 9 + Wed Jun 11 |
| 6 — Spark UI hands-on | Wed Jun 11 or Fri Jun 13 |
| 2.7–2.9 (window fns, arrays, joins) | Fri Jun 13 + Mon Jun 16 |
| 8 — Structured Streaming | Wed Jun 18 + Fri Jun 20 |
| 9 — DLT | Mon Jun 23 |
| 10 — Unity Catalog | Wed Jun 25 |
| 11 — Data Modeling | Fri Jun 27 + Mon Jun 30 |
| 12 — Databricks-Specific | Wed Jul 2 |
| AWS CCP exam-ready | Jul 18 |
| Apply-ready | Oct 15 |

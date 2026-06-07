# PySpark & Databricks Learning Sessions

Personal learning repository for PySpark, Spark internals, Delta Lake, Structured Streaming, Unity Catalog, and Databricks — structured for FAANG/top-tier Data Engineer interviews.

## Who this is for
Data Engineers targeting Databricks, Google, Meta, Apple, Netflix, and similar top-tier companies.

## Structure

```
├── easy/               # Core DataFrame API — filter, groupBy, joins, nulls, strings, dates
├── medium/             # Window functions, arrays, structs, pivot, advanced joins
├── hard/               # Performance tuning, Delta Lake, UDFs, temporal analytics
├── spark_internals/    # Architecture, shuffles, AQE, caching, salting, Spark UI
├── delta_lake/         # Transaction log, time travel, MERGE, OPTIMIZE, VACUUM
├── streaming/          # Structured Streaming, watermarks, exactly-once, checkpoints
├── unity_catalog/      # Namespace, grants, lineage, governance
├── data_modeling/      # Star schema, SCDs, medallion architecture
└── resources/          # Reference docs, checklists, interview prep notes
```

## Learning Path
See [LEARNING_PATH.md](LEARNING_PATH.md) for the full curriculum with progress tracking.

## Progress
- ✅ Spark Architecture & Mental Model
- ✅ Core DataFrame API (select, filter, groupBy, join, agg)
- ✅ Partitions, Shuffles, repartition vs coalesce
- ✅ Join Strategies (BHJ, SMJ, SHJ)
- ✅ AQE, Caching, Salting
- 🟡 Catalyst/Tungsten, Spark UI hands-on
- ⏳ Delta Lake, Structured Streaming, Unity Catalog, Data Modeling

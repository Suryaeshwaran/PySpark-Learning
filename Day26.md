#### 📘  Day26: Incremental ETL & Data Refresh (Append / Merge Pipelines) 
---
#### 🎯 Goal:
Learn how to **load only new or changed data** (instead of full loads), simulating real-world ETL refresh scenarios using **Append** and **Merge (Upsert)** logic.
#### 🧩 Concepts to Cover

**1. Incremental Loads vs Full Loads**

	- Full Load: Replace the entire dataset each time.
	- Incremental Load: Load only new or updated data since the last run.
**2. Two Strategies**

	- Append Mode: Add only new rows.
	- Merge Mode (Upsert): Add new + update existing records (by key).
**3. Data Identification**

	- Using columns like last_updated_at, created_at, or a surrogate key (e.g. id).
**4. Delta Table or Parquet Simulation**

	- If no Delta Lake: simulate merge using join and overwrite logic.
	- If Delta is available: use deltaTable.merge() for efficient upsert.


#### ⚙️ Mini Project Setup

Let’s extend the same ETL folder you used earlier (/project/real_world_etl).

**Step 1:  Prepare Historical Data**
``` python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("IncrementalETL").getOrCreate()

data_old = [
    (1, "Alice", "2024-01-01", 1000),
    (2, "Bob", "2024-01-03", 1500),
    (3, "Charlie", "2024-01-05", 1200)
]

df_old = spark.createDataFrame(data_old, ["id", "name", "date", "amount"])
df_old.show()
```
**Step 2:  Simulate New Batch (with new + updated rows)**
``` python
data_new = [
    (2, "Bob", "2024-01-03", 1800),     # updated
    (3, "Charlie", "2024-01-05", 1200), # unchanged
    (4, "David", "2024-01-07", 900)     # new record
]

df_new = spark.createDataFrame(data_new, ["id", "name", "date", "amount"])
df_new.show()
```
**Step 3:  🔸 Append Mode (Simple Incremental Add)**

Keep only new IDs not in old data.
``` python
df_append = df_new.join(df_old, "id", "left_anti")
df_incremental = df_old.union(df_append)
df_incremental.show()
```
**Step 4: 🔹 Merge Mode (Upsert Logic)**

Simulate update + insert using join conditions.
``` python
from pyspark.sql import functions as F

df_merge = df_old.alias("old").join(df_new.alias("new"), on="id", how="outer") \
    .select(
        F.coalesce(F.col("new.id"), F.col("old.id")).alias("id"),
        F.coalesce(F.col("new.name"), F.col("old.name")).alias("name"),
        F.coalesce(F.col("new.date"), F.col("old.date")).alias("date"),
        F.coalesce(F.col("new.amount"), F.col("old.amount")).alias("amount")
    )

df_merge.show()
```
**Step 5:  Store the Final Output**
``` python
output_path = "/project/real_world_etl/incremental_data/final_merge"
df_merge.write.mode("overwrite").parquet(output_path)
```

#### 💡 Exercises:

**Exercise 1:** Modify logic to detect only new or updated rows using a last_updated_at timestamp column.

**Goal:**  Detect **only new or updated rows** from a source dataset based on the last_updated_at timestamp column and load/merge them into the target table (data warehouse, Delta table, etc.).

**✅ Concept: We usually maintain:**
- Source table — raw incoming data (could be daily snapshot).
- Target table — historical store of processed data.
- Logic — compare last_updated_at in source vs. target.

Step 1: Create source & target DataFrames
Step 2: Detect new or updated rows

Use left join and filter where:
- The record is new (not in target), or
- The record is updated (timestamp newer).

Here is the code:
``` python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_timestamp

spark = SparkSession.builder.appName("IncrementalLoad").getOrCreate()

# Simulated Source Data (new incoming snapshot)
source_data = [
    (1, "Alice", "HR", "2025-10-12 09:00:00"),  # updated
    (2, "Bob", "IT", "2025-10-12 08:00:00"),    # same
    (3, "Carol", "Finance", "2025-10-12 09:10:00")  # new
]
source_df = spark.createDataFrame(source_data, ["emp_id", "name", "dept", "last_updated_at"]) \
    .withColumn("last_updated_at", to_timestamp(col("last_updated_at")))

# Simulated Target Data (existing historical data)
target_data = [
    (1, "Alice", "HR", "2025-10-12 08:30:00"),
    (2, "Bob", "IT", "2025-10-12 08:00:00")
]
target_df = spark.createDataFrame(target_data, ["emp_id", "name", "dept", "last_updated_at"]) \
    .withColumn("last_updated_at", to_timestamp(col("last_updated_at")))

incremental_df = source_df.alias("src") \
    .join(target_df.alias("tgt"), col("src.emp_id") == col("tgt.emp_id"), "left") \
    .filter(
        (col("tgt.emp_id").isNull()) |  # new record
        (col("src.last_updated_at") > col("tgt.last_updated_at"))  # updated record
    ) \
    .select("src.*")

incremental_df.show()
```
**Output:**
``` bash
+------+-----+--------+-------------------+
|emp_id| name|    dept|     last_updated_at|
+------+-----+--------+-------------------+
|     1|Alice|      HR|2025-10-12 09:00:00|
|     3|Carol| Finance|2025-10-12 09:10:00|
+------+-----+--------+-------------------+
```
**Exercise 2:** Implement the same pipeline using Delta Lake’smerge() API (if available).

**Goal:** Use Delta Lake’s merge() API to perform upserts — automatically inserting new rows and updating existing rows based on a unique key (e.g., emp_id) and change detection (last_updated_at).

This replaces manual join/filter logic from Exercise 1.

#### 🧩 Setup (Same as Before):

We’ll reuse the source and target data but save the target as a Delta table.

This requires the Delta package to be installed:
``` bash
pip install delta-spark
```
Here is the code:
``` python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from delta.tables import DeltaTable

spark = SparkSession.builder \
    .appName("DeltaMergeExample") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Source snapshot (new data)
source_data = [
    (1, "Alice", "HR", "2025-10-12 09:00:00"),  # updated
    (2, "Bob", "IT", "2025-10-12 08:00:00"),    # same
    (3, "Carol", "Finance", "2025-10-12 09:10:00")  # new
]
source_df = spark.createDataFrame(source_data, ["emp_id", "name", "dept", "last_updated_at"]) \
    .withColumn("last_updated_at", to_timestamp(col("last_updated_at")))

# Target table (existing Delta)
target_data = [
    (1, "Alice", "HR", "2025-10-12 08:30:00"),
    (2, "Bob", "IT", "2025-10-12 08:00:00")
]

target_df = spark.createDataFrame(target_data, ["emp_id", "name", "dept", "last_updated_at"]) \
    .withColumn("last_updated_at", to_timestamp(col("last_updated_at")))

# Save target as Delta table
target_path = "/tmp/delta/employee_data"
target_df.write.format("delta").mode("overwrite").save(target_path)
```
**🔄 Perform Delta Merge (Upsert)**
``` python
# Load Delta table
delta_target = DeltaTable.forPath(spark, target_path)

# Use Delta merge() for incremental update
delta_target.alias("tgt").merge(
    source_df.alias("src"),
    "tgt.emp_id = src.emp_id"
).whenMatchedUpdate(
    condition="src.last_updated_at > tgt.last_updated_at",  # update only newer
    set={
        "name": "src.name",
        "dept": "src.dept",
        "last_updated_at": "src.last_updated_at"
    }
).whenNotMatchedInsertAll()  # insert new records
.execute()

# Result Check

final_df = spark.read.format("delta").load(target_path)
final_df.show()
```
**Spark-submit:**
``` bash
spark-submit \
  --packages io.delta:delta-spark_2.13:4.0.0 \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  /Users/sureshkumar/myspark/week4/real_world_etl_project/deltaMergeExample.py
```
**Output:**
``` bash
+------+-----+-------+-------------------+
|emp_id| name|   dept|    last_updated_at|
+------+-----+-------+-------------------+
|     1|Alice|     HR|2025-10-12 09:00:00|
|     2|  Bob|     IT|2025-10-12 08:00:00|
|     3|Carol|Finance|2025-10-12 09:10:00|
+------+-----+-------+-------------------+
```

**⚡ Observation:**

Incremental ETL helps you handle large datasets efficiently by loading only changes, reducing processing time and cost.

#### 🌟 Summary 

You’ve now mastered Incremental ETL & Data Refresh:

⚙️ Detect Changes using last_updated_at timestamps or surrogate keys.

➕ Append Pipelines to load only new records efficiently.

🔄 Merge / Upsert Logic to update existing + insert new data seamlessly.

💡 Delta Lake merge() API for real-world, production-grade data refresh handling.
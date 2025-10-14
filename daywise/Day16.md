#### Day16: Spark Persistence: Cache & Checkpointing
---

#### 1. Why Persistence Matters
- Every time you run an action (collect(), count(), show()), Spark recomputes the entire lineage unless cached.
- This is expensive if:
	- The dataset is reused multiple times.
	- The transformations are heavy (e.g., joins, shuffles).

**Solution** → Cache or persist the dataset so Spark can reuse it.

#### 2. Cache vs Persist
- cache() = shorthand for persist(MEMORY_ONLY)
- persist(storage_level) = more control:
	- MEMORY_ONLY (default, fastest, may recompute if not enough memory)
	- MEMORY_AND_DISK (spills to disk if memory is insufficient)
	- DISK_ONLY (no memory use, always disk)
	- MEMORY_ONLY_SER (serialized, saves space, uses CPU to deserialize)

👉 Use cache() when dataset fits comfortably in memory.

👉 Use persist(MEMORY_AND_DISK) for large datasets that may not fit.

#### 3. Checkpointing

- Checkpointing writes data to a reliable storage system(HDFS, S3, Local disk)
- Unlike caching, it truncate lineage - useful for very long RDD chains(Prevent huge DAGs)
- Setup:
```python
spark.sparkContext.setCheckpointDir("/tmp/checkpoint")
df.checkpoint()
```
- Expensive, but essential for iterative algorithms (eg., ML training loops).

#### Example to try:
- Create example.py with below code
``` python
from pyspark.sql import SparkSession
from pyspark import StorageLevel

spark = SparkSession.builder.appName("PersistenceExample").getOrCreate()

rdd = spark.sparkContext.parallelize(range(1, 10**6))

# transformation
squared = rdd.map(lambda x: x * x)

# persist in memory
squared.persist(StorageLevel.MEMORY_ONLY)

print("First count (triggers computation):", squared.count())
print("Second count (uses cache):", squared.count())

spark.stop()
```
👉 On first count(), Spark computes everything.

👉 On second count(), Spark fetches from memory - No recomputation.

Check Spark UI > Storage tab to see cached RDD. (http://localhost:4040/storage) 

**Output:**
``` bash
First count (triggers computation): 999999
Second count (uses cache): 999999
```
#### Best Practices

- Always unpersist when you no longer need cached data:
``` python
squared.unpersist()
```
- Cache reusable, expensive-to-compute datasets (e.g., results of joins, filtered big tables).
- Use checkpointing if lineage is huge or recursive (GraphX, ML loops).

#### Hands-on Tasks for Today
``` bash
spark-submit --master "local[*]" example.py 2>&1 | tee -a spark_output.log
```
**Task 1:**
- Run the above script with and without cache() → observe recomputation time.

_Below is the execution output:_

**With cache()**
``` bash
example.py:14, took 890.521042 ms
First count (triggers computation): 999999

example.py:15, took 43.232042 ms
Second count (uses cache): 999999
```
**Without cache()**
``` bash
example.py:14, took 1249.8275 ms
First count (triggers computation): 999999

example.py:16, took 46.968208 ms
Second count (uses cache): 999999
```
**Task 2:**
- Try persist(StorageLevel.MEMORY_AND_DISK) with a dataset bigger than your Mac’s RAM → see spill to disk.
``` python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, to_date
from pyspark.storagelevel import StorageLevel

# Start Spark Session
spark = SparkSession.builder \
        .appName("Employee_Bonus") \
        .master("local[*]") \
        .getOrCreate()

# Read employees.csv
df = spark.read.csv("/Users/sureshkumar/myspark/PySpark-Learning/files/employees.csv", header=True, inferSchema=True)

# Ensure hire_date is treated as a date type
df = df.withColumn("hire_date", to_date("hire_date", "yyyy-MM-dd"))

# Create Dataset and add all columns using nested withColumn
employees = df.select("employee_id", "first_name", "salary", "department_id", "hire_date") \
        .withColumn("bonus", col("salary") * 0.1) \
        .withColumn("total_compensation", col("salary") + col("bonus")) \
        .withColumn("hire_year", year("hire_date"))

# Enable persistence for the 'employees' DataFrame using persist()
employees.persist(StorageLevel.MEMORY_AND_DISK)

# Now, perform an action to trigger the computation and caching
# For example, count the number of rows
employees.count() 

# You can now perform subsequent actions on the persisted DataFrame, like .show(), without re-computing from the start.
employees.show(100)

# To remove the cached data from memory, use unpersist()
employees.unpersist()
```
**Execution:**
``` bash
spark-submit --master "local[*]" persist_memory_disk.py 2>&1 | tee -a persist_memory_disk_output.log
# This will have whole shell output in log file

spark-submit --master "local[*]" persist_memory_disk.py > out.log
# This log file will have the .show() output alone
```
**Task 3:**
- Setup checkpointing → observe that lineage gets truncated in Spark UI.
``` python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, to_date

# Start Spark Session
spark = SparkSession.builder \
        .appName("Employee_Bonus_Checkpoint") \
        .master("local[*]") \
        .getOrCreate()

# ✅ Setup checkpoint directory (must be reliable storage, here local path for demo)
spark.sparkContext.setCheckpointDir("/tmp/spark-checkpoints")

# Read employees.csv
df = spark.read.csv(
    "/Users/sureshkumar/myspark/PySpark-Learning/files/employees.csv",
    header=True,
    inferSchema=True
)

# Ensure hire_date is treated as a date type
df = df.withColumn("hire_date", to_date("hire_date", "yyyy-MM-dd"))

# Transformations
employees = df.select("employee_id", "first_name", "salary", "department_id", "hire_date") \
        .withColumn("bonus", col("salary") * 0.1) \
        .withColumn("total_compensation", col("salary") + col("bonus")) \
        .withColumn("hire_year", year("hire_date"))

# ✅ Apply checkpointing (this truncates lineage)
employees_checkpointed = employees.checkpoint()

# Trigger action → will execute DAG and materialize checkpoint
print("Row count:", employees_checkpointed.count())

# Now subsequent actions do NOT depend on full lineage
employees_checkpointed.show(20)

spark.stop()
```
**Execution:**
```
spark-submit --master "local[*]" checkpointing.py > checkpointing_out.log
```
This sets a directory where checkpoint data is stored. "/tmp/spark-checkpoints"

**Task 4:**
- persist() (caching, keeps lineage) vs checkpoint() (cuts lineage)
``` python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, to_date
from pyspark.storagelevel import StorageLevel

# Start Spark Session
spark = SparkSession.builder \
        .appName("Persist_vs_Checkpoint") \
        .master("local[*]") \
        .getOrCreate()

# ✅ Setup checkpoint directory
spark.sparkContext.setCheckpointDir("/tmp/spark-checkpoints")

# Read employees.csv
df = spark.read.csv(
    "/Users/sureshkumar/myspark/PySpark-Learning/files/employees.csv",
    header=True,
    inferSchema=True
)

# Ensure hire_date is treated as a date type
df = df.withColumn("hire_date", to_date("hire_date", "yyyy-MM-dd"))

# Transformations (expensive lineage)
employees = df.select("employee_id", "first_name", "salary", "department_id", "hire_date") \
        .withColumn("bonus", col("salary") * 0.1) \
        .withColumn("total_compensation", col("salary") + col("bonus")) \
        .withColumn("hire_year", year("hire_date"))

# -------------------------------
# Case 1: Persist
# -------------------------------
employees_persisted = employees.persist(StorageLevel.MEMORY_AND_DISK)

print("\n=== Persist Example ===")
print("Row count:", employees_persisted.count())  # First action triggers compute + cache
employees_persisted.show(10)  # Uses cache

# -------------------------------
# Case 2: Checkpoint
# -------------------------------
employees_checkpointed = employees.checkpoint()

print("\n=== Checkpoint Example ===")
print("Row count:", employees_checkpointed.count())  # Triggers compute + writes to /tmp/spark-checkpoints
employees_checkpointed.show(10)  # Reads from checkpoint

# Cleanup persisted DataFrame
employees_persisted.unpersist()

spark.stop()
```
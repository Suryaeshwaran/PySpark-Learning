#### 📘 Day20: Joins & Partitioning Strategies
---

#### 1. Spark Joins

In Spark, joins work much like SQL, but under the hood they can cause shuffles (expensive operations).

**Common Types of Joins:** _(Re-calling from Day#10)_
- Inner Join – matching rows only.
- Left / Right Outer Join – all rows from one side + matches.
- Full Outer Join – all rows from both sides.
- Cross Join – Cartesian product.
- Semi / Anti Join – filter-only behaviour.

**Example:**
``` python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Day20_Joins").getOrCreate()

employees = [(1, "Alice", 10), (2, "Bob", 20), (3, "Cathy", 10)]
departments = [(10, "HR"), (20, "Finance")]

df1 = spark.createDataFrame(employees, ["emp_id", "name", "dept_id"])
df2 = spark.createDataFrame(departments, ["dept_id", "dept_name"])

# Inner Join
df1.join(df2, on="dept_id", how="inner").show()
```
**Execution:**
``` bash
python3 join.py
#or
spark-submit --master "local[*]" join.py
```

#### 2. Partitioning & Shuffle in Joins
- When two large DataFrames are joined, Spark may need to shuffle rows across the cluster to group same key.
- This is why joins can be slow.

👉 Partitioning strategies help control this.

#### 3. Partitioning Strategies for Joins

1. Shuffle Hash Join (default) 
2. Broadcast Hash Join
3. Repartitioning Before Join

**Shuffle Hash Join (default)**
- Spark redistributes both DataFrames by join key.
- Costly when both are large.

**Broadcast Hash Join**
- If one DataFrame is small enough, Spark broadcasts it to all executors.
- Avoids shuffle.
``` python
from pyspark.sql.functions import broadcast

df1.join(broadcast(df2), "dept_id").show()
```
**Repartitioning Before Join**
- Sometimes, explicitly repartitioning helps:
``` python
df1 = df1.repartition("dept_id")
df2 = df2.repartition("dept_id")
df1.join(df2, "dept_id").show()
```
**Skew Handling**

- If one key is “hot” (lots of rows), joins may hang.
- Spark configs like spark.sql.adaptive.skewJoin.enabled=true can split skewed partitions.

**Verify in Spark UI**
- Open http://localhost:4040 → SQL tab.
- Look at Details -> physical plan (Spark shows if it used BroadcastHashJoin, ShuffledHashJoin, or SortMergeJoin).

#### Example Code: 
- **Shuffle Join vs Broadcast Join vs Repartition**
``` python
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

# Start Spark
spark = SparkSession.builder \
    .appName("Day20_Joins") \
    .master("local[*]") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \ # disable auto broadcast for demo
    .getOrCreate()

# Employees (big table)
employees = [
    (1, "Alice", 10, 50000),
    (2, "Bob", 20, 60000),
    (3, "Cathy", 10, 70000),
    (4, "David", 30, 80000),
    (5, "Eva", 40, 90000),
]
# Departments (small table)
departments = [
    (10, "HR"),
    (20, "Finance"),
    (30, "IT"),
    (40, "Marketing"),
]

df1 = spark.createDataFrame(employees, ["emp_id", "name", "dept_id", "salary"])
df2 = spark.createDataFrame(departments, ["dept_id", "dept_name"])

# 🔹 1. Normal Shuffle Join
print("\n===== Normal Shuffle Join =====")
shuffle_join = df1.join(df2, "dept_id", "inner")
shuffle_join.explain(True)
shuffle_join.show()

# 🔹 2. Broadcast Join
print("\n===== Broadcast Join =====")
broadcast_join = df1.join(broadcast(df2), "dept_id", "inner")
broadcast_join.explain(True)
broadcast_join.show()

# 🔹 3. Repartitioned Join
print("\n===== Repartitioned Join =====")
df1_re = df1.repartition("dept_id")
df2_re = df2.repartition("dept_id")

repartitioned_join = df1_re.join(df2_re, "dept_id", "inner")
repartitioned_join.explain(True)
repartitioned_join.show()

input("\nPress Enter to stop Spark...")
spark.stop()

```
**Execution:**
``` bash
python3 all-in-one.py
# All respective output will be printed in shell itself.
```

🔹 How to Inspect the Difference

[Check under == Physical Plan == Section]

**Shuffle Join will show:**
```
Exchange hashpartitioning(dept_id,...)
```
- → Spark is shuffling data across executors.

**Broadcast Join will show:**
``` bash
BroadcastHashJoin
BroadcastExchange
```
→ Spark broadcasted the small dataset to all executors.

**Repartition will show:**

It will show Exchange hashpartitioning(dept_id, …) only once per DataFrame (not multiple unnecessary shuffles).
``` bash
Exchange hashpartitioning(dept_id,...)
```
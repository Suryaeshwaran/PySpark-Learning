#### Day17: Spark Shuffles
---

#### What is a Shuffle?
- A shuffle is the process of redistributing data across partitions.
- It happens when a transformation requires data from multiple partitions to be grouped/combined.

Spark has to:

- Write intermediate data to disk.
- Transfer it over the network to the right executor.
- Read it back into memory for the next stage.

👉 Shuffles are expensive (network + disk + serialization).

**Example:** (No shuffle vs Shuffle)

``` python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ShuffleExample").getOrCreate()

rdd = spark.sparkContext.parallelize([
    ("a", 1), ("b", 1), ("a", 1), ("b", 1), ("c", 1)
], 2)  # start with 2 partitions

# Narrow transformation (no shuffle)
mapped = rdd.mapValues(lambda x: x + 1)
print("Map result:", mapped.collect())

# Wide transformation (causes shuffle)
reduced = rdd.reduceByKey(lambda a, b: a + b)
print("ReduceByKey result:", reduced.collect())

input("\nOpen Spark UI at http://localhost:4040 to see stages.\nPress Enter to exit...")

spark.stop()
```
👉 _Check the below details In the Spark UI:_
- mapValues runs in one stage (no shuffle).
- reduceByKey causes two stages:
	- Stage 1: map side (prepare shuffle data).
	- Stage 2: reduce side (aggregate).

#### Narrow vs Wide Transformations
**Narrow transformations :**
  
  Each output partition depends on a single input partition.

👉 Example: map, filter, flatMap
- No shuffle required.   
- Fast, stays within executor.

**Wide transformations :**

  Each output partition depends on multiple input partitions.
  
👉 Example: groupByKey, reduceByKey, join, distinct

- Triggers a shuffle.
- Causes stage boundary in DAG.

_When Shuffles Happen_
``` bash
- groupByKey(), reduceByKey(), aggregateByKey()
- join() between two datasets
- distinct()
- repartition()
```
**Hands-on Tasks for Today**

- Create a small DataFrame with employees + departments → join them and observe shuffle.
``` python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
	.appName("Dept_Avg_Salary") \
	.getOrCreate()

# Read CSV into DataFrame 
df1 = spark.read.csv("/Users/sureshkumar/myspark/PySpark-Learning/files/employees.csv", header=True, inferSchema=True)
df2 = spark.read.csv("/Users/sureshkumar/myspark/PySpark-Learning/files/departments.csv", header=True, inferSchema=True)

# Create temporary view from DataFrame
df1.createOrReplaceTempView("employees")
df2.createOrReplaceTempView("departments")

# SQL Query
result = spark.sql("""
	SELECT d.department_name,
		SUM(e.salary) AS salary,
		ROUND(AVG(e.salary),2) AS average_salary
	FROM employees e
	JOIN departments d
	ON e.department_id = d.department_id
	GROUP BY d.department_name
	HAVING AVG(e.salary) > 60000
 """)

result.show()

input("\n Open Spark UI at http://localhost:4040 and check Stages/Shuffle metrics.\nPress Enter to exit...")

spark.stop()
```
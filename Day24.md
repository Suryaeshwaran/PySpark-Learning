#### 📘 Day24: Window Functions & Analytics in PySpark
---
#### 🎯 Goal:
Learn how to perform advanced analytics — ranking, running totals, lead/lag comparisons — using window functions in PySpark.
#### 1. What Are Window Functions?
Window functions let you perform aggregations across a defined set of rows (a window) without collapsing them into a single row (unlike groupBy).

For example:
- Find top N salaries per department
- Compute running totals
- Compare each row with the previous or next record
#### 2. Import Required Packages
``` python
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, rank, dense_rank, lag, lead, sum
```
#### 3. Sample Data

``` python
spark = SparkSession.builder.appName("WindowFunctionsExample").getOrCreate()

data = [
    ("Sales", "John", 5000),
    ("Sales", "Alice", 6000),
    ("Sales", "Bob", 4000),
    ("HR", "Mary", 4500),
    ("HR", "Steve", 5500),
    ("HR", "Nancy", 3500),
]

columns = ["department", "employee", "salary"]
df = spark.createDataFrame(data, columns)
df.show()
```
#### 4. Example 1 — Rank Employees by Salary Within Department

``` python
windowSpec = Window.partitionBy("department").orderBy(col("salary").desc())

df.withColumn("rank", rank().over(windowSpec)).show()
```
🟩 Output → shows rank (1,2,3) within each department.

#### 5. Example 2 — Top 1 Employee (Highest Salary) per Department

``` python
df.withColumn("rank", row_number().over(windowSpec)) \
  .filter(col("rank") == 1) \
  .show()
```
#### 6. Example 3 — Running Total of Salaries per Department

``` python
windowSpec2 = Window.partitionBy("department").orderBy("salary").rowsBetween(Window.unboundedPreceding, 0)

df.withColumn("running_total", sum("salary").over(windowSpec2)).show()
```

#### 7. Example 4 — Compare with Previous Employee (Using lag)

``` python
windowSpec3 = Window.partitionBy("department").orderBy("salary")

df.withColumn("prev_salary", lag("salary", 1).over(windowSpec3)) \
  .withColumn("salary_diff", col("salary") - col("prev_salary")) \
  .show()
```

#### 8. Example 5 — Compare with Next Employee (Using lead)

``` python
df.withColumn("next_salary", lead("salary", 1).over(windowSpec3)) \
  .withColumn("diff_next", col("next_salary") - col("salary")) \
  .show()
```

#### 🧩 Exercise:

- Find average salary per department and display each employee’s deviation from that average. 
- Display top 2 earners per department. 
- Compute cumulative salary percentage within each department.

``` python
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, avg, rank, sum, round

# Spark session already created
spark = SparkSession.builder.appName("WindowFunctionsExample").getOrCreate()

data = [
    ("Sales", "John", 5000),
    ("Sales", "Alice", 6000),
    ("Sales", "Bob", 4000),
    ("HR", "Mary", 4500),
    ("HR", "Steve", 5500),
    ("HR", "Nancy", 3500),
]

columns = ["department", "employee", "salary"]
df = spark.createDataFrame(data, columns)

# 🧩 1️⃣ Find average salary per department and deviation for each employee
windowDept = Window.partitionBy("department")

df_avg_dev = df.withColumn(
	"avg_salary", avg("salary").over(windowDept)
	).withColumn(
	"deviation", col("salary") - col("avg_salary")
	)
print("=== Average salary per department and deviation ===")
df_avg_dev.show()

# 🧩 2️⃣ Display top 2 earners per department
windowRank = Window.partitionBy("department").orderBy(col("salary").desc())

df_top2 = df.withColumn("rank", rank().over(windowRank)).filter(col("rank") <= 2)

print("=== Top 2 earners per department ===")

df_top2.show()

# 🧩 3️⃣ Compute cumulative salary percentage within each department

windowCum = Window.partitionBy("department").orderBy(col("salary").desc())\
	.rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_cum = df.withColumn(
	"total_salary", sum("salary").over(windowDept)
	).withColumn(
	"cumulative_salary", sum("salary").over(windowCum)
	).withColumn(
	"cumulative_percent", round((col("cumulative_salary") / col("total_salary")) * 100, 2)
	)

print("=== Cumulative salary percentage per department ===")
df_cum.show()
```
**💡 Output Summary:**
``` bash
=== Average salary per department and deviation ===
+----------+--------+------+----------+---------+                               
|department|employee|salary|avg_salary|deviation|
+----------+--------+------+----------+---------+
|        HR|    Mary|  4500|    4500.0|      0.0|
|        HR|   Steve|  5500|    4500.0|   1000.0|
|        HR|   Nancy|  3500|    4500.0|  -1000.0|
|     Sales|    John|  5000|    5000.0|      0.0|
|     Sales|   Alice|  6000|    5000.0|   1000.0|
|     Sales|     Bob|  4000|    5000.0|  -1000.0|
+----------+--------+------+----------+---------+
```
**1️⃣ Deviation from average** → Shows how much each employee earns above/below their department’s mean.
``` bash
=== Top 2 earners per department ===
+----------+--------+------+----+
|department|employee|salary|rank|
+----------+--------+------+----+
|        HR|   Steve|  5500|   1|
|        HR|    Mary|  4500|   2|
|     Sales|   Alice|  6000|   1|
|     Sales|    John|  5000|   2|
+----------+--------+------+----+
```
**2️⃣ Top 2 earners** → Uses rank() so ties are handled properly.
``` bash
=== Cumulative salary percentage per department ===
+----------+--------+------+------------+-----------------+------------------+
|department|employee|salary|total_salary|cumulative_salary|cumulative_percent|
+----------+--------+------+------------+-----------------+------------------+
|        HR|   Steve|  5500|       13500|             5500|             40.74|
|        HR|    Mary|  4500|       13500|            10000|             74.07|
|        HR|   Nancy|  3500|       13500|            13500|             100.0|
|     Sales|   Alice|  6000|       15000|             6000|              40.0|
|     Sales|    John|  5000|       15000|            11000|             73.33|
|     Sales|     Bob|  4000|       15000|            15000|             100.0|
+----------+--------+------+------------+-----------------+------------------+
```
**3️⃣ Cumulative %** → Adds up salaries in descending order to show what % of department total each employee contributes cumulatively.

**⚡ Observation:**

- Window functions don’t reduce rows like groupBy — they add analytical columns that depend on other rows in the same partition.
- Window functions are **analytical**, not **aggregative** — they enrich each row with context from other rows within a defined window.
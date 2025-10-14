#### 📘 Day09: Work with DataFrames (select, filter, withColumn).
---

- A data frame is a structured representation of data.
- DataFrame manipulation becomes your daily bread.
Below is a compact, runnable step-by-step lesson with code, mini-exercises and practical tips you can run locally with pyspark.

**0) Start Spark & sample data**

``` python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Day9-DataFrameBasics") \
    .master("local[*]") \
    .getOrCreate()

data = [
    ("Alice", "Sales", 3000, "2021-06-01"),
    ("Bob",   "Sales", 4600, "2020-02-15"),
    ("Cathy", "HR",    3900, "2019-09-12"),
    ("Dan",   "IT",    5000, "2021-05-23"),
    ("Eve",   None,    2500, None)
]
cols = ["name","dept","salary","join_date"]
df = spark.createDataFrame(data, schema=cols)

df.show(truncate=False)
df.printSchema()
```

**1) Select: pick columns & expressions**

- select returns another DataFrame. 
- Use F.col() for programmatic access, use select(F.col(...)) when building columns programmatically.
- Use selectExpr for quick SQL expressions.

``` python
# single column
df.select("name").show()

# multiple columns + expression + alias
df.select("name", (F.col("salary")/1000).alias("salary_k")).show()

# SQL-like expressions
df.selectExpr("name", "salary * 1.10 as salary_plus_10pct").show()
```

**2) filter/ where: row filtering (same thing)**

``` python
# simple
df.filter(F.col("salary") > 3500).show()

# multiple conditions
df.filter((F.col("dept") == "Sales") & (F.col("salary") > 3500)).show()

# IN / LIKE
df.filter(F.col("dept").isin("Sales","IT")).show()
df.filter(F.col("name").like("A%")).show()
```
Notes: use bitwise & / | (not and/or) and wrap each condition in parentheses.

**3) withColumn:  add or modify columns**

- withColumn returns a new DataFrame (immutable transformation).
``` python
# add derived column
df2 = df.withColumn("salary_k", F.col("salary") / 1000)

# modify an existing column (overwrite)
df3 = df.withColumn("salary", (F.col("salary") * 1.05).cast("double"))

# conditional column (using when/otherwise)
df.withColumn(
    "salary_band",
    F.when(F.col("salary") < 3000, "Low")
     .when(F.col("salary") < 4500, "Mid")
     .otherwise("High")
).show()
```
- If you call withColumn multiple times, you're creating successive DataFrames — assign to a variable each time if you want to keep changes (df = df.withColumn(...)).

**4) Common useful DataFrame ops (short cheats)**

``` python
# rename / drop
df.withColumnRenamed("join_date", "start_date").show()
df.withColumnRenamed("join_date", "start_date").drop("start_date").show()

# cast type
df.withColumn("salary", F.col("salary").cast("double")).printSchema()

# distinct, orderBy, limit
df.select("dept").distinct().show()
df.orderBy(F.col("salary").desc()).show()
df.limit(3).show()

# groupBy + agg
df.groupBy("dept").agg(
    F.count("*").alias("cnt"),
    F.round(F.avg("salary"),2).alias("avg_salary")
).show()

# missing values
df.na.drop(subset=["join_date"])
df.na.fill({"dept": "Unknown", "salary": 0})

# union (same schema)
# df.unionByName(other_df)

# SQL API
df.createOrReplaceTempView("employees")
spark.sql("SELECT name, salary FROM employees WHERE salary > 3000").show()
```
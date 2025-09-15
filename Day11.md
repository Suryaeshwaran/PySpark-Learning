#### Day11: Use Spark SQL
---
Spark SQL is a powerful, expressive way to run SQL over your Spark DataFrames / tables.
We will see: how to create views, run queries, do joins (including broadcast), use window functions and UDFs, inspect plans, cache results and save outputs — plus a few short practice tasks.


**0) Start Spark (with optional Hive support)**

``` python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Day11_SparkSQL") \
    # .enableHiveSupport()     # uncomment if your Spark is configured with Hive metastore
    .getOrCreate()
```

**1) Create sample data and register temp views**

``` python
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
data = [
    (1, "Alice", 10, 65000.0),
    (2, "Bob",   20, 52000.0),
    (3, "Cathy", 10, 72000.0),
    (4, "Dave",  30, 48000.0),
    (5, "Eve",   20, 90000.0),
]
schema = StructType([
    StructField("emp_id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("dept_id", IntegerType(), False),
    StructField("salary", DoubleType(), False),
])
employees = spark.createDataFrame(data, schema)
departments = spark.createDataFrame([(10,"HR"), (20,"Eng"), (30,"Sales")], ["dept_id","dept_name"])

# register views
employees.createOrReplaceTempView("employees")
departments.createOrReplaceTempView("departments")
# global view (available across sessions): employees.createGlobalTempView("employees_global")
```

**2) Basic SQL — select, filter, order**

``` python
# run SQL and get a DataFrame back
high_paid = spark.sql("SELECT emp_id, name, salary FROM employees WHERE salary > 60000 ORDER BY salary DESC")

high_paid.show()

# Equivalent DataFrame API: 

employees.filter("salary > 60000").select("emp_id","name","salary").orderBy("salary", ascending=False)
```

**3) Aggregations & GROUP BY**

``` python
agg = spark.sql("""
  SELECT dept_id, COUNT(*) AS cnt, ROUND(AVG(salary),2) AS avg_salary, MAX(salary) AS max_salary
  FROM employees
  GROUP BY dept_id
  ORDER BY avg_salary DESC
""")
agg.show()
```

**4) JOINs (with SQL broadcast hint for small tables)**

``` python
# regular join via SQL
spark.sql("""
  SELECT e.emp_id, e.name, e.salary, d.dept_name
  FROM employees e
  JOIN departments d
    ON e.dept_id = d.dept_id
""").show()

# broadcast hint (useful when departments is small)
spark.sql("""
  SELECT /*+ BROADCAST(d) */ e.emp_id, e.name, e.salary, d.dept_name
  FROM employees e
  JOIN departments d
    ON e.dept_id = d.dept_id
""").show()
```

Hint syntax /*+ BROADCAST(tableAlias) */ instructs Spark to broadcast that table to all executors.

**5) Window functions (SQL)**

``` python
# top earner per department using ROW_NUMBER()
spark.sql("""
  SELECT emp_id, name, dept_id, salary
  FROM (
    SELECT e.*, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC) as rn
    FROM employees e
  ) t
  WHERE rn = 1
""").show()
```

**6) Register and use a UDF in SQL**

``` python
# register Python UDF
def tax_calc(sal):
    return float(sal) * 0.10

spark.udf.register("tax", tax_calc)

# use the udf inside SQL
spark.sql("SELECT name, salary, tax(salary) AS tax_amount FROM employees").show()
```
Note: UDFs can hurt performance — prefer built-in functions when possible.

**7) Cache / persist a table (good for repeated SQL queries)**

``` python
# cache the view (DataFrame-level)
employees.cache()
employees.count()   # materialize the cache

# or cache via catalog on a table/view name
spark.catalog.cacheTable("employees")
# later: spark.catalog.uncacheTable("employees")
```

**8) Inspect query plans and debug**

``` python
# show detailed plan for a SQL query
q = spark.sql("SELECT e.dept_id, COUNT(*) FROM employees e GROUP BY e.dept_id")
q.explain(True)   # verbose plan: parsed/analyzed/optimized/physical
```

**9) Read from / write to persistent formats**
``` python
# read CSV or parquet and register view
df = spark.read.option("header","true").option("inferSchema","true").csv("/path/to/employees.csv")
df.createOrReplaceTempView("employees_csv")

# write SQL result to parquet partitioned by dept_id
spark.sql("""
  SELECT dept_id, COUNT(*) AS cnt, AVG(salary) AS avg_salary
  FROM employees
  GROUP BY dept_id
""").write.mode("overwrite").partitionBy("dept_id").parquet("/tmp/employees_agg_parquet")

# if Hive enabled, you can also:
# spark.sql("CREATE DATABASE IF NOT EXISTS demo")
# spark.sql("CREATE TABLE IF NOT EXISTS demo.employees_agg AS SELECT ...")

# For verification
dfin = spark.read.parquet("/tmp/employees_agg_parquet")
dfin.show()
```

#### Day13: DataFrame API Exercises
---
**Exercise 1: Nested withColumn Transformation**
- Dataset: employees (employee_id, name, salary, job_id, hire_date)
- Add a column bonus = 10% of salary.
- Add another column total_compensation = salary + bonus.
- Extract the year from hire_date as a new column hire_year.
``` python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, to_date

# Start Spark Session
spark = SparkSession.builder \
        .appName("Employee_Bonus") \
        .master("local[*]") \
        .getOrCreate()

# Read employees.csv
df = spark.read.csv("employees.csv", header=True, inferSchema=True)

# Ensure hire_date is treated as a date type
df = df.withColumn("hire_date", to_date("hire_date", "yyyy-MM-dd"))

# Create Dataset and add all columns using nested withColumn
employees = df.select("employee_id", "first_name", "salary", "department_id", "hire_date") \
        .withColumn("bonus", col("salary") * 0.1) \
        .withColumn("total_compensation", col("salary") + col("bonus")) \
        .withColumn("hire_year", year("hire_date"))

employees.show()
```
**Exercise 2: Advanced Filtering**

Get employees with:
- Salary between 50,000 and 80,000.
- Hired after 2015.
- Belonging to departments 80, 90, or 100.
``` python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, year

# Start Spark Session
spark = SparkSession.builder \
        .appName("Mid_Salary_Employee") \
        .master("local[*]") \
        .getOrCreate()

# Read employees.csv
df = spark.read.csv("employees.csv", header=True, inferSchema=True)

# multiple conditions on filter

df.filter(F.col("salary").between(50000, 80000) &
 (year("hire_date") > 2015) &
 (col("department_id").isin([80, 90, 100]))).show()
```

**Exercise 3: Window Functions**
- For each department, rank employees by salary.
- Find the top 2 highest-paid employees per department.
- Add a column avg_salary_by_dept showing the average salary within each department.
``` python
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, dense_rank, row_number, col, avg, round

# Start Spark Session
spark = SparkSession.builder \
        .appName("Rank_Salary_Department") \
        .master("local[*]") \
        .getOrCreate()

# Read employees.csv
df = spark.read.csv("employees.csv", header=True, inferSchema=True)

# define Window spec
window_spec = Window.partitionBy("department_id").orderBy(col("salary").desc())
avg_window = Window.partitionBy("department_id")

# Rank Employee & Top 2 rank on each department
top_2_rank_per_dept = df.withColumn("salary_rank", rank().over(window_spec))\
        .withColumn("avg_salary_by_dept", round(avg("salary").over(avg_window),2)) \
        .filter(col("salary_rank") <= 2) \
        .select("employee_id", "first_name", "salary", "job_id", "salary_rank", "avg_salary_by_dept") \
        .orderBy("department_id", "salary_rank") \

top_2_rank_per_dept.show()
```

**Exercise 4: GroupBy with Multiple Aggregations**

For each department:
- Count employees.
- Find average, max, and min salary.
- Show results sorted by average salary descending.
``` python
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, max, min, col, round

# Start Spark Session
spark = SparkSession.builder \
        .appName("Department_Aggregations") \
        .master("local[*]") \
        .getOrCreate()

# Load employees.csv (assuming you already have hire_date fixed earlier)
df = spark.read.csv("employees.csv", header=True, inferSchema=True)

# Group by department_id and aggregate
dept_stats = df.groupBy("department_id") \
	.agg(
	count("*").alias("employee_count"),
	round(avg("salary"),2).alias("avg_salary"),
	max("salary").alias("max_salary"),
	min("salary").alias("min_salary")
	)\
	.orderBy(col("avg_salary").desc())

dept_stats.show()
```
- Include a join to print department_name from departments.csv
``` python
# Load departments.csv
df2 = spark.read.csv("departments.csv", header=True, inferSchema=True)

# Join with departments to get department_name
result = dept_stats.join(df2, "department_id", "left") \
    .select("department_id", "department_name", "employee_count", "avg_salary", "max_salary", "min_salary") \
    .orderBy(col("department_id"))

result.show(truncate=False)
```
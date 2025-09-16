#### Day12: SQL Practise Exercise
---
**Exercise 1:** 

-  Create Temp View and list Top 2 earners in each department
- Load a CSV of employees (or use the sample above), create a temp view employees_csv. 
- Run SQL to find the top-2 earners in each department.
``` python
from pyspark.sql import SparkSession

# Start SparkSession

spark = SparkSession.builder \
        .appName("TempViewExample") \
        .getOrCreate()

# read CSV into DataFrame
df = spark.read.csv("employees.csv", header=True, inferSchema=True)

# Create or replace temp view
df.createOrReplaceTempView("employees_csv")

# Create ranked list result & print
result = spark.sql("""
        WITH ranked_employees AS (
        SELECT job_id,
                employee_id,
                salary,
                RANK() OVER (PARTITION BY job_id ORDER BY salary DESC) AS rank_in_dept
        FROM employees_csv
        )
 SELECT *
 FROM ranked_employees
 WHERE rank_in_dept <= 2
""")

result.show()
```
**Exercise 2:**
- Join employees with departments using a broadcast hint
- and measure difference via explain(True).
``` python
from pyspark.sql import SparkSession

# Start SparkSession

spark = SparkSession.builder \
	.appName("TempViewExample") \
	.getOrCreate()

# read CSV into DataFrame
df = spark.read.csv("employees.csv", header=True, inferSchema=True)

# Create or replace temp view
df.createOrReplaceTempView("employees_csv")

dept = spark.read.csv("departments.csv", header=True, inferSchema=True)
dept.createOrReplaceTempView("departments_csv")

#spark.sql("SELECT department_id from departments_csv").show()

join_broadcast = spark.sql("""
    SELECT /*+ BROADCAST(d) */ e.employee_id, e.salary, d.department_name
    FROM employees_csv e
    JOIN departments_csv d
      ON e.department_id = d.department_id
""")

join_broadcast.explain(True)
```

**Exercise 3:**
- Using UDF to Create pay_grade
- Register a UDF to label pay grade (LOW, MID, HIGH) and use it in an aggregation: SELECT pay_grade, COUNT(*) FROM ... GROUP BY pay_grade.
``` python
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Start SparkSession

spark = SparkSession.builder \
	.appName("TempViewExample") \
	.getOrCreate()

# read CSV into DataFrame
df = spark.read.csv("employees.csv", header=True, inferSchema=True)

# Create or replace temp view
df.createOrReplaceTempView("employees_csv")

# Define Python function
def get_pay_grade(salary):
    if salary < 40000:
        return "LOW"
    elif salary < 60000:
        return "MID"
    else:
        return "HIGH"

# Register UDF with Spark
spark.udf.register("pay_grade_udf", get_pay_grade, StringType())

result = spark.sql("""
	SELECT pay_grade_udf(salary) AS pay_grade,
	COUNT(*) AS Count
	FROM employees_csv
	GROUP BY pay_grade
 """)

result.show()
```

**Exercise 4:** 
- Joining Temp Views (Employees & Departments)
``` python
from pyspark.sql import SparkSession

# Start SparkSession

spark = SparkSession.builder \
	.appName("TempViewExample") \
	.getOrCreate()

# read CSV into DataFrame
df = spark.read.csv("employees.csv", header=True, inferSchema=True)
df.createOrReplaceTempView("employees_csv")

df2 = spark.read.csv("departments.csv", header=True, inferSchema=True)
df2.createOrReplaceTempView("departments_csv")

# join two views (employees_csv and departments_csv) and select specific columns with aliases

result = spark.sql("""
	SELECT e.first_name as Name,
		d.department_name as Department
	FROM employees_csv e
	JOIN departments_csv d
	ON e.department_id = d.department_id
 """)

result.show()
```
**Exercise 5:** 
- Find the Highest Paid Employee in Each Department
``` python
from pyspark.sql import SparkSession

# Start Spark session
spark = SparkSession.builder \
    .appName("SQLExercise") \
    .getOrCreate()

# Assume views already exist:
# employees_csv and departments_csv

# SQL query using window function
result = spark.sql("""
    WITH ranked_employees AS (
        SELECT e.employee_id,
               e.first_name,
               e.salary,
               d.department_name,
               RANK() OVER (PARTITION BY e.department_id ORDER BY e.salary DESC) AS rnk
        FROM employees_csv e
        JOIN departments_csv d
          ON e.department_id = d.department_id
    )
    SELECT employee_id,
           first_name,
           salary,
           department_name
    FROM ranked_employees
    WHERE rnk = 1
""")

result.show()
```
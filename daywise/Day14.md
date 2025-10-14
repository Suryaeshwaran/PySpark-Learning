#### 📘 Day14: Spark SQL Exercises
---
**Exercise 1: Join & Aggregation Challenge**
- Tables: employees, departments.
- Find each department’s total salary and average salary.
- Display only those departments where avg salary > 60,000.
``` python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
	.appName("Dept_Avg_Salary") \
	.getOrCreate()

# Read CSV into DataFrame
df1 = spark.read.csv("/Users/sureshkumar/myspark/PySpark-Learning/files/employees.csv", header=True, inferSchema=True)
df2 = spark.read.csv("/Users/sureshkumar/myspark/PySpark-Learning/files/departments.csv", header=True, inferSchema=True)

# Save DataFrame as a Spark SQL Table
df1.write.saveAsTable("employees")
df2.write.saveAsTable("departments")

# SQL Query

result = spark.sql("""
	SELECT d.department_name,
		SUM(e.salary) AS total_salary,
		ROUND(AVG(e.salary),2) AS avg_salary
	FROM employees e
	JOIN departments d
	ON e.department_id = d.department_id
	GROUP BY d.department_name
	HAVING AVG(e.salary) > 60000
 """)

result.show()

# Drop the Tables
spark.sql("DROP TABLE IF EXISTS employees")
spark.sql("DROP TABLE IF EXISTS departments")
```
**Exercise 2: Case When**
- Create a column salary_band in SQL:
``` bash
	< 40,000 → "Low"
	40,000–70,000 → "Medium"
	> 70,000 → "High"
```

``` python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .appName("EMP_Salary_Band") \
        .getOrCreate()

# Read CSV into DataFrame
df1 = spark.read.csv("/Users/sureshkumar/myspark/PySpark-Learning/files/employees.csv", header=True, inferSchema=True)

# Create temporary view from DataFrame
df1.createOrReplaceTempView("employees")

spark.sql("""
    SELECT employee_id,
           first_name,
           last_name,
           salary,
           CASE
               WHEN salary < 40000 THEN 'Low'
               WHEN salary BETWEEN 40000 AND 70000 THEN 'Medium'
               WHEN salary > 70000 THEN 'High'
           END AS salary_band
    FROM employees
""").show(truncate=False)
```
**Exercise 3: Subqueries**
- Find employees whose salary is above the overall average salary.
- Find departments where the number of employees is greater than the average number of employees across all departments.
``` python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .appName("Subqueries") \
        .getOrCreate()

# Read CSV into DataFrame
df1 = spark.read.csv("/Users/sureshkumar/myspark/PySpark-Learning/files/employees.csv", header=True, inferSchema=True)
df2 = spark.read.csv("/Users/sureshkumar/myspark/PySpark-Learning/files/departments.csv", header=True, inferSchema=True)

# Create temporary view from DataFrame
df1.createOrReplaceTempView("employees")
df2.createOrReplaceTempView("departments")

print("\n 1. Employees whose salary is above the overall average salary:")

spark.sql("""
    SELECT first_name, salary, department_id
    FROM employees
    	WHERE salary > (SELECT AVG(salary) FROM employees)
""").show()

print("\n 2. Departments where the number of employees is greater than the average number of employees across all departments.")

spark.sql("""
	SELECT d.department_name, COUNT(e.employee_id) AS emp_count
	FROM employees e
	JOIN departments d
		ON e.department_id = d.department_id
	GROUP BY d.department_name,d.department_id
	HAVING COUNT(e.employee_id) > ( 
		SELECT AVG(dept_count)
		FROM ( SELECT COUNT(*) AS dept_count FROM employees GROUP BY department_id 
	) tmp
  )
""").show()
```
**Exercise 4: CTEs (Common Table Expressions)**
- Create a CTE to calculate department-wise average salary.
- Join it back to employees to list only those who earn more than their department’s average.
``` python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .appName("Subquiries") \
        .getOrCreate()

# Read CSV into DataFrame
df1 = spark.read.csv("/Users/sureshkumar/myspark/PySpark-Learning/files/employees.csv", header=True, inferSchema=True)

# Create temporary view from DataFrame
df1.createOrReplaceTempView("employees")

spark.sql("""
    WITH dept_avg AS (
        SELECT department_id, AVG(salary) AS avg_salary
        FROM employees
        GROUP BY department_id
        )
   SELECT e.employee_id,
        e.first_name,
        e.salary,
        e.department_id,
        ROUND(d.avg_salary,2) AS avg_salary
  FROM employees e
  JOIN dept_avg d
        ON e.department_id = d.department_id
        WHERE e.salary > d.avg_salary
""").show()
```
**Exercise 5: Window Function in SQL**
- Rank employees by hire_date (earliest to latest).
- For each department, get the employee who joined earliest.
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .appName("Window_Functions") \
        .getOrCreate()

# Read CSV into DataFrame
df1 = spark.read.csv("/Users/sureshkumar/myspark/PySpark-Learning/files/employees.csv", header=True, inferSchema=True)

# Create temporary view from DataFrame
df1.createOrReplaceTempView("employees")

print ("\n Early Joiner of the Department :")
spark.sql("""
     WITH ranked_emps AS (
	SELECT e.employee_id,
		CONCAT(e.first_name,' ',e.last_name) AS emp_name,
		e.department_id,
		e.hire_date,
		ROW_NUMBER() OVER (PARTITION BY e.department_id ORDER BY e.hire_date ASC) AS rn
	FROM employees e
    )
    SELECT employee_id, emp_name, department_id, hire_date
    FROM ranked_emps
    WHERE rn = 1
""").show()
```
**Explanation:**
1. ROW_NUMBER() OVER (PARTITION BY dept ORDER BY hire_date ASC)
	- Gives a rank starting from 1 for the earliest hire in each department.
2. CTE (ranked_emps)
	- Stores employees with their row number.
3. Final SELECT
	- Picks only rn = 1, i.e., the earliest hire per department.
#### Day10: Practice joins & aggregations
---

**Small practice exercises**

- Note: employees.csv is available in files directory in GitHub
- main code
``` python
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Initialise a Spark Session
spark = SparkSession.builder.appName("EmployeeDataImport").getOrCreate()

# Read employees.csv
df = spark.read.csv("employees.csv", header=True, inferSchema=True)

```
**Select + filter:** 
- Read employees.csv, select first_name, dept, salary_k where salary > 50000, and save to CSV.

``` python
df.filter(F.col("salary") > 50000).select("first_name","job_id", (F.col("salary") / 1000).alias("salary_k")).show()
```
**Add column:** 
- Create experience_years from join_date (use to_date and date functions) and bucket into junior/mid/senior.
``` python
# 1. Use withColumn to calculate 'experience_years'

df_with_years = df.withColumn(
    "experience_years",
    F.floor(
        F.datediff(F.current_date(), F.to_date(F.col("hire_date"), "yyyy-MM-dd")) / 365.25
    )
)

# 2. Use withColumn again to create the 'experience_level' column
df_final = df_with_years.withColumn(
    "experience_level",
    F.when(F.col("experience_years") >= 10, "Senior")
    .when(F.col("experience_years") >= 5, "Mid-Level")
    .otherwise("Junior")
)

# 3. Select and display the final result
df_final.select("first_name", "hire_date", "experience_years", "experience_level").show()
```

**Group & agg:** 
- For each dept compute count and avg_salary, sort by avg_salary.
``` python
df.groupBy('job_id') \
  .agg(
      F.count('job_id').alias('count'),
      F.avg('salary').alias('avg_salary')
  ) \
  .orderBy('avg_salary') \
  .show() 
```

**Join:** 
- Join with a small dept_codes DataFrame and broadcast it.
1.  Start with employees DataFrame
2. Create the small lookup DataFrame (dept_codes)
3. Broadcast join
	- Use broadcast() to force Spark to push the small table to all worker nodes (avoids expensive shuffles).
``` python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

spark = SparkSession.builder \
    .appName("JoinExample") \
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
print("\nPrint 'data' :")
df.show()

dept_data = [
    ("Sales", "S1"),
    ("HR",    "H1"),
    ("IT",    "I1")
]
dept_cols = ["dept", "dept_code"]

dept_df = spark.createDataFrame(dept_data, dept_cols)
print("\nPrint 'dept_df' :")
dept_df.show()


# left join employees with dept_codes
joined = df.join(broadcast(dept_df), on="dept", how="left")

joined.show()
```
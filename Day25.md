#### Day25: Real-World ETL Project using PySpark
---
#### 🎯 Goal:
Simulate a real-world **ETL (Extract–Transform–Load)** data pipeline using PySpark, where you’ll:
1. Extract data from multiple sources (CSV + JSON)
2. Transform (clean, join, aggregate)
3. Load it into a final Parquet file for analytics.

#### 🧩 Scenario: Retail Sales Analysis

A retail company stores:
- Customer data → customers.csv
- Transaction data → transactions.json

Perform below task:

Clean, join, and prepare an analytics-ready dataset showing each customer’s total spend and transaction count, stored as Parquet.

#### 🧱 Step 1 — Setup Spark & Input Data

``` python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, round

spark = SparkSession.builder.appName("Day25_ETL_Project").getOrCreate()

```
Create Data:
- Create data/customer.csv
``` bash
customer_id,name,city
101,"Alice Johnson",New York
102,"Bob Smith",Los Angeles
103,"Carol Davis",Chicago
104,"David Lee",Houston
```
- Create data/transactions.json
``` bash
{"customer_id": 101, "amount": 45.99, "product": "Laptop Bag"}
{"customer_id": 102, "amount": 12.50, "product": "Coffee Mug"}
{"customer_id": 101, "amount": 199.99, "product": "Wireless Keyboard"}
{"customer_id": 104, "amount": 75.00, "product": "Headphones"}
{"customer_id": 103, "amount": 5.99, "product": "Notebook"}
{"customer_id": 102, "amount": 250.00, "product": "Smart Watch"}
```
#### 🧮 Step 2 — Extract

``` python
customers_df = spark.read.csv("customers.csv", header=True, inferSchema=True)
transactions_df = spark.read.json("transactions.json")

print("Customers:")
customers_df.show()

print("Transactions:")
transactions_df.show()
```
#### ⚙️ Step 3 — Transform

**Clean Nulls**
``` python
transactions_df = transactions_df.na.drop(subset=["customer_id", "amount"])

```
**Join**
``` python
joined_df = customers_df.join(transactions_df, on="customer_id", how="inner")

```
**Aggregate**
``` python
agg_df = joined_df.groupBy("customer_id", "name", "city") \
    .agg(
        round(_sum("amount"), 2).alias("total_spent"),
        count("product").alias("total_transactions")
    )

agg_df.show()
```

#### 💾 Step 4 — Load (Save as Parquet)

``` python
agg_df.write.mode("overwrite").parquet("output/retail_analytics.parquet")
```
#### 🔍 Step 5 — Validate Load

``` python
result_df = spark.read.parquet("output/retail_analytics.parquet")
result_df.show()
```

#### ⚡ Observation

This ETL pipeline demonstrates how PySpark automates the entire data workflow -  from raw input (CSV/JSON) → transformation → clean analytics output (Parquet).
- Such pipelines form the backbone of real-world data engineering.

#### 💪 Exercise

1. Add a “high spender” flag for customers who spent > ₹100.
2. Save final data as both Parquet and ORC, compare file sizes.
3. Read from Parquet and print final result.

Here is the code:
``` python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, when, lit, round

spark = SparkSession.builder.appName("ETL_Project02").getOrCreate()

# Extract

customers_df = spark.read.csv("file:///Users/sureshkumar/myspark/week4/etl/data/customers.csv", header=True, inferSchema=True)
transactions_df = spark.read.json("file:///Users/sureshkumar/myspark/week4/etl/data/transactions.json")

# --- 1. Aggregate Transactions ---
# Group by customer_id and sum the amount

spending_df = transactions_df.groupBy("customer_id").agg(round(sum("amount"),2).alias("total_spent"))

# --- 2. Join and Add Flag ---
# Join the customers and spending dataframes

final_df = customers_df.join(spending_df, on="customer_id", how="left")

# Replace any null total_spent (for customers with no transactions) with 0.00
# and add the 'high_spender' flag

final_df = final_df.withColumn(
	"total_spent",
	when(col("total_spent").isNull(), lit(0.00)).otherwise(col("total_spent"))
	).withColumn("high_spender", col("total_spent") > 100.00 )


# Define paths for the output (replace with your desired paths)
parquet_output_path = "file:///Users/sureshkumar/myspark/week4/etl/output/customer_data_parquet"
orc_output_path = "file:///Users/sureshkumar/myspark/week4/etl/output/customer_data_orc"

# Save as Parquet
# Parquet is often the default and recommended format
final_df.write.mode("overwrite").parquet(parquet_output_path)
print(f"Data saved to Parquet at: {parquet_output_path}")

# Save as ORC
# ORC is another optimized columnar format
final_df.write.mode("overwrite").orc(orc_output_path)
print(f"Data saved to ORC at: {orc_output_path}")

# Read & Print from Parquet

result_df = spark.read.parquet(parquet_output_path)
print(f"Print from Parquet:")
result_df.show()
```
**Output:**
``` bash
Data saved to Parquet at: file:///Users/sureshkumar/myspark/week4/etl/output/customer_data_parquet
Data saved to ORC at: file:///Users/sureshkumar/myspark/week4/etl/output/customer_data_orc
Print from Parquet:
+-----------+-------------+-----------+-----------+------------+
|customer_id|         name|       city|total_spent|high_spender|
+-----------+-------------+-----------+-----------+------------+
|        101|Alice Johnson|   New York|     245.98|        true|
|        102|    Bob Smith|Los Angeles|      262.5|        true|
|        103|  Carol Davis|    Chicago|       5.99|       false|
|        104|    David Lee|    Houston|       75.0|       false|
+-----------+-------------+-----------+-----------+------------+
```
#### 🌟 Summary

You’ve now mastered a complete end-to-end data pipeline:
- 🧩 Extract (CSV, JSON)
- ⚙️ Transform (join, aggregate, clean)
- 💾 Load (Parquet, ORC)
- ✅ Validate + Automate
That’s exactly what real PySpark-based ETL engineers do in production 💪
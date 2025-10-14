#### 📘  Day27: Streaming Basics, Streaming with Kafka
---
#### 🎯 Goal:
Understand **real-time data processing** in PySpark — how to **read, process, and write continuously streaming data** from Kafka or other live sources.

**Apache Kafka** is a distributed event store and stream-processing platform, originally developed at LinkedIn.

#### ⚙️ Local Streaming Example:

Here is the code to print word count on console:
``` python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("StreamingBasics").getOrCreate()

# Step 1: Create socket stream
stream_df = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Step 2: Split text into words
word_count = stream_df.selectExpr("explode(split(value, ' ')) as word") \
    .groupBy("word").count()

# Step 3: Write results to console every 5s
query = word_count.writeStream \
    .outputMode("update") \
    .format("console") \
    .trigger(processingTime='5 seconds') \
    .start()

query.awaitTermination()
```
**How to Execute this ?**

**On Terminal 1 :**
``` bash
nc -l 9999
my first line
```
**On Terminal 2 :**
``` bash
spark-submit stream_count_console.py
```
**Output will show:**
``` bash
   "numOutputRows" : 3

# if you scroll you can see:
+-----+-----+
| word|count|
+-----+-----+
|line|    1|
|   my|    1|
|first|    1|
+-----+-----+
```
#### ⚙️ Setting Up Kafka locally:

**1️⃣ Install Kafka and Zookeeper**
``` bash
# Install Kafka (includes Zookeeper)

brew install kafka

# Verify installation
kafka-server-start --version
```
**Set Home path:**
``` bash
To start kafka now and restart at login:
  brew services start kafka
Or, if you don't want/need a background service you can just run:
  /opt/homebrew/opt/kafka/bin/kafka-server-start /opt/homebrew/etc/kafka/server.properties

# From above set below as home path

echo 'export PATH=$PATH:/opt/homebrew/opt/kafka/bin' >> ~/.zshrc
source ~/.zshrc
```

**1️⃣ Start Kafka broker ( On Terminal 1 and leave it running)**
``` bash
kafka-server-start /opt/homebrew/etc/kafka/server.properties
```
Now Kafka is running without Zookeeper on localhost:9092.

**2️⃣ Create a topic (On Terminal 2)**
``` bash
kafka-topics --create --topic sales_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

```
- sales_topic will be ready to use.

**3️⃣ Produce messages**
``` bash
kafka-console-producer --topic sales_topic --bootstrap-server localhost:9092

```
**Type JSON messages:**
``` json
{"order_id":"1001","product":"Laptop","quantity":2,"price":1200.5}
```
**4️⃣ Consume messages (On Terminal 3)**
``` bash
kafka-console-consumer --topic sales_topic --from-beginning --bootstrap-server localhost:9092
```
Keep post some more messages on Producer and monitor on Consumer:
``` json
{"order_id":"1003","product":"Keyboard","quantity":3,"price":45.0}
{"order_id":"1004","product":"Monitor","quantity":1,"price":250.0}
{"order_id":"1005","product":"USB-C Cable","quantity":10,"price":10.0}
```
To Clean this Topic and start with today exercise delete and create the topic:
``` bash
# Delete topic
kafka-topics --bootstrap-server localhost:9092 --delete --topic sales_topic

# Recreate topic
kafka-topics --bootstrap-server localhost:9092 --create --topic sales_topic --partitions 1 --replication-factor 1
```
#### ⚙️ Streaming Example on Kafka:

**1️⃣ Prerequisites**
1. Kafka broker is running on localhost:9092 (you already did that).
2. sales_topic exists and is empty.
3. PySpark installed with Kafka package

**Code:** _stream_with_kafka.py_

``` python
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# ------------------------------
# 1️⃣ Define JSON schema
# ------------------------------
schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("product", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True)
])

# ------------------------------
# 2️⃣ Create Spark session
# ------------------------------
spark = SparkSession.builder \
    .appName("KafkaStreamingExample") \
    .getOrCreate()

# ------------------------------
# 3️⃣ Read from Kafka
# ------------------------------
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sales_topic") \
    .load()

# ------------------------------
# 4️⃣ Parse JSON messages
# ------------------------------
json_df = df.selectExpr("CAST(value AS STRING) as json") \
            .select(from_json(col("json"), schema).alias("data")) \
            .select("data.*")

# ------------------------------
# 5️⃣ Write stream to console
# ------------------------------
query = json_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()
```
**✅ How to run it**
- Incase any version miss-match error can come, fix it then run it.
``` bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 stream_with_kafka.py
```
**✅ Steps to test**
1. Make sure Kafka broker is running (localhost:9092) and topic sales_topic exists.
2. In a separate terminal, produce some JSON messages:
``` bash
kafka-console-producer --topic sales_topic --bootstrap-server localhost:9092

```
**Example messages:**
``` json
{"order_id":"1001","product":"Laptop","quantity":2,"price":1200.5}
{"order_id":"1002","product":"Mouse","quantity":5,"price":25.0}
```
3. You will see the parsed output in your Spark console immediately.

**Output:** _(truncated)_
``` bash
-------------------------------------------
Batch: 1
-------------------------------------------

+--------+-------+--------+------+
|order_id|product|quantity| price|
+--------+-------+--------+------+
|    1001| Laptop|       2|1200.5|
+--------+-------+--------+------+
 

"sales_topic" : {
 "numOutputRows" : 1

Batch: 2
-------------------------------------------
+--------+-------+--------+-----+
|order_id|product|quantity|price|
+--------+-------+--------+-----+
|    1002|  Mouse|       5| 25.0|
+--------+-------+--------+-----+
```
#### 🧩 Exercises
1. Read real-time sales data from Kafka and calculate running total sales.

2. Write the stream to a Parquet sink every 30 seconds.

3. Add a checkpoint directory and verify fault recovery.

**🔹 Exercise 1:** 
Read real-time sales data from Kafka and calculate running total sales.

**Code:** _running_sales_kafka.py_
``` python
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# 1️⃣ Define schema
schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("product", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True)
])

# 2️⃣ Create Spark Session
spark = SparkSession.builder.appName("KafkaSalesRunningTotal").getOrCreate()

# 3️⃣ Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sales_topic") \
    .load()

# 4️⃣ Parse JSON and compute total
sales_df = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("total", expr("quantity * price"))

# 5️⃣ Aggregate running total
running_total = sales_df.groupBy().agg(_sum("total").alias("running_total_sales"))

# 6️⃣ Write to console
query = running_total.writeStream \
    .outputMode("complete") \
    .format("console") \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()
```
**Execution:**

**1. Start Kafka service:(Terminal 1)**
```
kafka-server-start /opt/homebrew/etc/kafka/server.properties
```
**2. Run spark-submit:(Terminal 2)**
``` bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 sales_total_stream.py
```
**3. Running producer: (Terminal 3)**
``` bash
kafka-console-producer --topic sales_topic --bootstrap-server localhost:9092

# send few messages
{"order_id":"1001","product":"Laptop","quantity":2,"price":1200.5}
{"order_id":"1002","product":"Mouse","quantity":5,"price":25.0}
{"order_id":"1003","product":"Keyboard","quantity":3,"price":45.0}
```
**Output:(Terminal 2)**
``` bash
-------------------------------------------
Batch: 1
-------------------------------------------
+-------------------+
|running_total_sales|
+-------------------+
|             2401.0|
+-------------------+
-------------------------------------------
Batch: 2
-------------------------------------------
+-------------------+
|running_total_sales|
+-------------------+
|             2526.0|
+-------------------+
-------------------------------------------
Batch: 3
-------------------------------------------
+-------------------+
|running_total_sales|
+-------------------+
|             2661.0|
+-------------------+
```

#### 🔹 Exercise 2: Write the stream to a Parquet sink every 30 seconds.

**Full script:** _sales_to_parquet_stream.py_
``` python
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# ------------------------------
# 1️⃣ Define JSON schema
# ------------------------------
schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("product", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True)
])

# ------------------------------
# 2️⃣ Create Spark session
# ------------------------------
spark = SparkSession.builder.appName("KafkaToParquetStream").getOrCreate()

# ------------------------------
# 3️⃣ Read from Kafka
# ------------------------------
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sales_topic") \
    .load()

# ------------------------------
# 4️⃣ Parse and transform
# ------------------------------
sales_df = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("total", expr("quantity * price"))

# ------------------------------
# 5️⃣ Write to Parquet every 30 seconds
# ------------------------------
query = sales_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "/Users/sureshkumar/myspark/week4/streaming/parquet_output") \
    .option("checkpointLocation", "/Users/sureshkumar/myspark/week4/streaming/checkpoint") \
    .trigger(processingTime="30 seconds") \
    .start()

query.awaitTermination()
```
**Execution:**
1. Kafka is already running
2. Run spark-submit:(Terminal 2)
``` bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 sales_to_parquet_stream.py
```
**3. Running producer: (Terminal 3)**
``` bash
kafka-console-producer --topic sales_topic --bootstrap-server localhost:9092

# send few messages
{"order_id":"1001","product":"Laptop","quantity":2,"price":1200.5}
{"order_id":"1002","product":"Mouse","quantity":5,"price":25.0}
{"order_id":"1003","product":"Keyboard","quantity":3,"price":45.0}
```
**Reader Script:** _read_parquet_output.py_
``` python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ReadParquetOutput").getOrCreate()

df = spark.read.parquet("file:///Users/sureshkumar/myspark/week4/streaming/parquet_output")
df.show()
```
**Output:**
``` bash
+--------+--------+--------+------+------+
|order_id| product|quantity| price| total|
+--------+--------+--------+------+------+
|    NULL|    NULL|    NULL|  NULL|  NULL|
|    1003|Keyboard|       3|  45.0| 135.0|
|    1001|  Laptop|       2|1200.5|2401.0|
|    1002|   Mouse|       5|  25.0| 125.0|
|    NULL|    NULL|    NULL|  NULL|  NULL|
+--------+--------+--------+------+------+
```
**⚡ Observation:** During this execution spark-submit doesn’t exit by itself. Even with Ctrl +c 

So, kill it manually.
``` bash
ps aux | grep spark

kill -9 <PID>
```

#### 🔹 Exercise 3: Add a checkpoint directory and verify fault recovery.

**Code file:** _kafka_parquet_checkpoint.py_
``` python
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# 1️⃣ Create SparkSession
spark = SparkSession.builder \
    .appName("KafkaToParquet") \
    .getOrCreate()

# 2️⃣ Define schema of JSON messages
schema = StructType([
    StructField("order_id", StringType()),
    StructField("product", StringType()),
    StructField("quantity", IntegerType()),
    StructField("price", DoubleType())
])

# 3️⃣ Read stream from Kafka topic
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sales_topic") \
    .option("startingOffsets", "latest") \
    .load()

# 4️⃣ Extract and parse JSON data
json_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 5️⃣ Add computed column: total_amount = quantity * price
sales_df = json_df.withColumn("total_amount", expr("quantity * price"))

# 6️⃣ Write stream to Parquet sink every 30 seconds
query = sales_df.writeStream \
    .format("parquet") \
    .option("path", "file:///Users/sureshkumar/myspark/week4/streaming/output/parquet_sales/") \
    .option("checkpointLocation", "file:///Users/sureshkumar/myspark/week4/streaming/checkpoint/parquet_sales/") \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .start()

# 7️⃣ Wait for the stream to terminate
query.awaitTermination()
```
**🧪 How to Run It**
``` bash
# start Kafka
kafka-server-start /opt/homebrew/etc/kafka/server.properties

# Start Spark streaming job
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 kafka_parquet_checkpoint.py

# start producer

kafka-console-producer --topic sales_topic --bootstrap-server localhost:9092
```
**Send JSON messages:**
``` json
{"order_id":"1001","product":"Laptop","quantity":2,"price":1200.5}
{"order_id":"1002","product":"Mouse","quantity":5,"price":25.0}
{"order_id":"1003","product":"Keyboard","quantity":3,"price":45.0}
```
**🪄 Verify Data:**
``` python

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("VerifyParquetOutput").getOrCreate()

df = spark.read.parquet("file:/Users/sureshkumar/myspark/week4/streaming/output/parquet_sales")
df.show()
```
**Output:**
``` bash
+--------+--------+--------+------+------------+
|order_id| product|quantity| price|total_amount|
+--------+--------+--------+------+------------+
|    1002|   Mouse|       5|  25.0|       125.0|
|    1003|Keyboard|       3|  45.0|       135.0|
|    NULL|    NULL|    NULL|  NULL|        NULL|
|    1001|  Laptop|       2|1200.5|      2401.0|
+--------+--------+--------+------+------------+
```
#### ⚡ Outcome

A real-time streaming ETL pipeline powered by Kafka + Spark Structured Streaming, handling continuous ingest, transformation, and persistence.

#### 🌟 Summary 

_⚡ You’ve now mastered Real-Time Streaming with PySpark:_

**🌊 Structured Streaming Fundamentals** — treating live data as continuous DataFrames.

**🧩 Kafka Integration** — ingesting real-time messages and parsing JSON payloads.

**🔁 Continuous Processing** — using triggers (.trigger(processingTime='5 seconds')) and checkpoints for fault tolerance.

**💾 Stream Sinks** — writing live data to console, Parquet, or Kafka for downstream systems.

**📊 Real-Time Aggregations** — computing running totals and metrics on streaming data.
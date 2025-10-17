#### 📘 Day29: Streaming CDC with Kafka
---
#### 🎯 Goal:
Build a real-time CDC (Change Data Capture) pipeline using PySpark Structured Streaming + Kafka.

You’ll simulate updates to customer/order data and push CDC events into Kafka → Spark will process those changes → update target data incrementally.

#### 🧩 Concepts You’ll Learn
1. CDC Basics – Insert / Update / Delete changes in streaming.
2. Kafka Source & Sink – Reading/writing CDC data from/to Kafka.
3. Merge Logic in Stream – Using Structured Streaming to update only changed records.

#### 🌊 Setup Recap
- Start Kafka broker ( On a Terminal  and leave it running)
``` bash
kafka-server-start /opt/homebrew/etc/kafka/server.properties
```
- Create topic
``` bash
kafka-topics --create --topic customers_cdc --bootstrap-server localhost:9092
```
- Messages will be JSON format, e.g.
``` json
{"id":1,"name":"Alice","email":"alice@example.com","op":"insert"}
{"id":1,"name":"Alice Smith","email":"alice.smith@example.com","op":"update"}
{"id":2,"name":"Bob","email":"bob@example.com","op":"insert"}
```

#### ⚙️ Exercise 1: Read CDC Stream from Kafka
- Create _read_cdc_stream_from_kafka._py
``` python
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType

spark = SparkSession.builder.appName("Streaming_CDC_Kafka").getOrCreate()

# Define schema
schema = StructType() \
    .add("id", IntegerType()) \
    .add("name", StringType()) \
    .add("email", StringType()) \
    .add("op", StringType())

# Read from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "customers_cdc") \
    .load()

# Parse JSON messages
df_parsed = df_raw.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

df_parsed.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .trigger(processingTime="10 seconds") \
    .start() \
    .awaitTermination()
```
**Execution:**
``` bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 read_cdc_stream_from_kafka.py
```
**Start Producers & send message: **
``` bash
kafka-console-producer --topic customers_cdc --bootstrap-server localhost:9092
```
**JSON**
``` json
{"id":1,"name":"Alice","email":"alice@example.com","op":"insert"}
{"id":1,"name":"Alice Smith","email":"alice.smith@example.com","op":"update"}
{"id":2,"name":"Bob","email":"bob@example.com","op":"insert"}
```
**Output:**
``` bash
-------------------------------------------
Batch: 1
-------------------------------------------
+---+-----+-----------------+------+
|id |name |email            |op    |
+---+-----+-----------------+------+
|1  |Alice|alice@example.com|insert|
+---+-----+-----------------+------+
-------------------------------------------
Batch: 2
-------------------------------------------
+---+-----------+-----------------------+------+
|id |name       |email                  |op    |
+---+-----------+-----------------------+------+
|1  |Alice Smith|alice.smith@example.com|update|
+---+-----------+-----------------------+------+
-------------------------------------------
Batch: 3
-------------------------------------------
+---+----+---------------+------+
|id |name|email          |op    |
+---+----+---------------+------+
|2  |Bob |bob@example.com|insert|
+---+----+---------------+------+
```
#### ⚙️ Exercise 2: Spark streaming job
- **Start Kafka**
``` bash
kafka-server-start /opt/homebrew/etc/kafka/server.properties
```
- **Create Topic:**
``` bash
kafka-topics --create --topic orders_cdc --bootstrap-server localhost:9092
```
**Code:** _spark_streaming_job.py_
``` python
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count, sum, expr
from pyspark.sql.types import StructType, StringType, IntegerType

spark = (SparkSession.builder
         .appName("CDC_Console_Stream")
         .getOrCreate())

# Define JSON schema
schema = (StructType()
          .add("order_id", IntegerType())
          .add("product", StringType())
          .add("quantity", IntegerType())
          .add("op", StringType()))

# Read stream from Kafka
df_raw = (spark.readStream
           .format("kafka")
           .option("kafka.bootstrap.servers", "localhost:9092")
           .option("subscribe", "orders_cdc")
           .load())

# Parse the Kafka value column
df = (df_raw
      .select(from_json(col("value").cast("string"), schema).alias("data"))
      .select("data.*"))

# 💡 Simple CDC analytics
# Count number of each operation and total quantity seen so far
cdc_stats = (df.groupBy("op")
              .agg(count("*").alias("event_count"),
                   sum("quantity").alias("total_quantity")))

# Stream results to console every 10 s
query = (cdc_stats.writeStream
          .outputMode("complete")
          .format("console")
          .option("truncate", "false")
          .trigger(processingTime="10 seconds")
          .start())

query.awaitTermination()
```

**Execution:**
``` bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 \
  spark_streaming_job.py
```
**Start Producer:**
``` bash
kafka-console-producer --topic orders_cdc --bootstrap-server localhost:9092
```
**Send JSON Message:**
``` json
{"order_id":1001,"product":"Laptop","quantity":2,"op":"insert"}
{"order_id":1002,"product":"Mouse","quantity":1,"op":"insert"}
{"order_id":1001,"product":"Laptop","quantity":3,"op":"update"}
{"order_id":1002,"product":"Mouse","quantity":0,"op":"delete"}
```
**Output:**
``` bash
-------------------------------------------
Batch: 1
-------------------------------------------
+------+-----------+--------------+
|op    |event_count|total_quantity|
+------+-----------+--------------+
|insert|1          |2             |
+------+-----------+--------------+
-------------------------------------------
Batch: 2
-------------------------------------------
+------+-----------+--------------+
|op    |event_count|total_quantity|
+------+-----------+--------------+
-------------------------------------------
|insert|2          |3             |
+------+-----------+--------------+
-------------------------------------------
Batch: 3
-------------------------------------------
+------+-----------+--------------+
|op    |event_count|total_quantity|
+------+-----------+--------------+
|update|1          |3             |
|insert|2          |3             |
+------+-----------+--------------+
Batch: 4
-------------------------------------------
+------+-----------+--------------+
|op    |event_count|total_quantity|
+------+-----------+--------------+
|update|1          |3             |
|delete|1          |0             |
|insert|2          |3             |
+------+-----------+--------------+
```
#### 🔹 Key Limitation

- Only thing that works with Parquet in streaming is append raw events as they arrive, without aggregations.
- Aggregations / stateful operations (like groupBy, sum, last, or CDC “merge” logic) require complete or update **output modes, which Parquet does NOT support**.

#### 💡 Conclusion

- True CDC with streaming + Parquet is not possible in Spark 4.0.1 without Delta.
- Due to limitation on version miss-match, we can only proceed with Print on Console.

#### 🌟 Summary:  Streaming CDC (Console Mode)

⚡ You’ve now mastered real-time CDC simulation using Kafka & PySpark Structured Streaming:

 **🧩 Kafka ingestion** - reading live JSON events from a Kafka topic (orders_cdc).

 **🔍 CDC event parsing** - extracted operation types (insert, update, delete) and fields with schema.

 **📊 Live aggregation** - maintained running counts and totals grouped by operation (groupBy + agg).

 **🖥️ Console sink output** - viewed real-time metrics every few seconds (true streaming behavior).

 **🚀 Checkpoint-free workflow** - avoided write-mode limitations and still observed live CDC updates.
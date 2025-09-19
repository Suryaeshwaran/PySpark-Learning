#### Day15: Spark execution model (Driver, Cluster manager, Executor)
---
**Apache Spark's** execution model is built around a distributed computing framework that efficiently processes large datasets across clusters.

#### Core Components :-
**Driver:**

- The process that runs your main(). (program/scripts)
- It builds the logical plan, creates the DAG, schedule task and collects results.

**Cluster Manager:**
- Allocates resources across the cluster.
- (can be Spark's Standalone manager, YARN, Mesos or Kubernetes)

**Executors:**
- Worker Processes(JVMs) that run on cluster nodes, executing tasks and storing data for your application.

**Execution Flow:**

_When you Submit a Spark application, the execution follows this pattern:_

**1. Job Submission:**

- The driver program creates a SparkContext and defines the computation as a series of transformations and actions on RDDs (Resilient Distributed Datasets) or DataFrames.

**2. DAG Creation:**

- Spark builds a DAG(Directed Acyclic Graph) of operations. 
- Transformations like map(), filter(), and join() are lazy - they don't execute immediately but build up this computation graph.

**3. Stage Division:**

- The DAG gets divided into stages at shuffle boundaries (operations like groupBy(), reduceByKey() that require data redistribution across partitions).

**4. Task Creation:**
- Each stage is broken down into tasks - one task per partition of data.

**5. Task Scheduling:**

- The driver sends tasks to executors based on data locality when possible (moving computation to where data resides).

**6. Execution:** 
- Executors run tasks in parallel, with each task processing one partition of data.

This execution model makes Spark particularly effective for complex analytics workloads, machine learning pipelines, and interactive data exploration where the same dataset might be accessed multiple times.

_Let's Begin..._

**Install Apache Spark:**(This is for MAC)
``` python
# Brew & Open JDK already installed
brew install apache-spark

# Set Environment Variables
export SPARK_HOME="/opt/homebrew/opt/apache-spark/libexec"
export PATH="$PATH:$SPARK_HOME/bin"
```
**Simple Example:**
- Create a script is named wordcount.py
``` python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("WordCount").getOrCreate()

data = ["apple orange", "apple banana", "banana orange orange"]

rdd = spark.sparkContext.parallelize(data)

word_counts = (
    rdd.flatMap(lambda line: line.split(" "))
       .map(lambda word: (word, 1))
       .reduceByKey(lambda a, b: a + b)
)

print(word_counts.collect())

spark.stop()
```
- Submit the program
``` bash
spark-submit --master "local[*]" wordcount.py
```
You should see the printed result like:
``` bash
[('apple', 2), ('orange', 3), ('banana', 2)]
```
- Other submit options:
``` bash
# Run with only 2 threads (to simulate executors):
spark-submit --master "local[2]" wordcount.py

# Increase driver memory:
spark-submit --master "local[*]" --driver-memory 2g wordcount.py

# Pass arguments to your script:
spark-submit wordcount.py input.txt output_dir
# (in your script, use sys.argv to read them)
```
- Check Spark Web UI
While the job runs, open:
``` bash
http://localhost:4040
```
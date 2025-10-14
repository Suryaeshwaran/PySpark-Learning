#### Day19: Resource Management & Job Scheduling
---

#### 1. Understand Spark Resources
- Spark jobs need CPU and Memory.
- These are split between:
	- Driver → coordinates the job.
	- Executors → do the actual work.

#### 2. Key Resource Configurations

When you run spark-submit, you can control resources:
``` bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 3 \
  --executor-cores 2 \
  --executor-memory 2G \
  --driver-memory 1G \
  wordcount.py
```

👉 Example above requests:

- 3 executors × 2 cores = 6 cores total.
- 3 executors × 2G memory = 6G memory total.

Other Details:
``` text
--num-executors → total executors requested.
--executor-cores → CPU cores per executor.
--executor-memory → memory per executor (excluding overhead).
--driver-memory → memory for the driver.
```

#### 3. Dynamic Allocation

Instead of fixing executors, Spark can scale up/down automatically:

Enable in spark-defaults.conf or with --conf:
``` bash
--conf spark.dynamicAllocation.enabled=true \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.minExecutors=2 \
--conf spark.dynamicAllocation.maxExecutors=10 \
--conf spark.dynamicAllocation.initialExecutors=2

```

🔑 Requires shuffle service running in YARN.

#### 4. Scheduling Modes

When multiple Spark jobs run:

- FIFO (First In First Out) → default, jobs run in order of submission.
- Fair Scheduler → resources shared fairly across jobs.

To Enable fair share we use below syntax while spark-submit:
``` bash
--conf spark.scheduler.mode=FAIR
```

Example:
```
spark-submit \
  --master yarn \
  --conf spark.scheduler.mode=FAIR \
  wordcount.py
```

In YARN:
-  Use pool configuration for finer control:
- Create fairscheduler.xml in $SPARK_HOME/conf/:
``` xml
<allocations>
  <pool name="production">
    <schedulingMode>FAIR</schedulingMode>
    <weight>2</weight>
    <minShare>2</minShare>
  </pool>
  <pool name="dev">
    <schedulingMode>FAIR</schedulingMode>
    <weight>1</weight>
    <minShare>1</minShare>
  </pool>
</allocations>
``` 

- Then tell Spark to use it:
``` bash
spark.scheduler.allocation.file=/path/to/fairscheduler.xml
```
- In your code, you can set a pool for specific jobs:
```
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "dev")
```

#### Verify Scheduling Mode

Check the Spark UI → Jobs Tab.

Under “Scheduling Mode”, it will show FIFO or FAIR.

If using pools, jobs will be grouped under pool names.

#### 5. Monitoring

- Spark UI (http://localhost:4040) → check executors, memory, tasks.
- YARN ResourceManager UI (http://localhost:8088) → check resource allocation across all jobs.

**Exercise:**

- Run thru deploy-mode client & cluster
``` bash
PYSPARK_PYTHON=$(which python3) \
PYSPARK_DRIVER_PYTHON=$(which python3) \
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 1 \
  --executor-cores 1 \
  --executor-memory 1G \
  --driver-memory 1G \
  wordcount.py

PYSPARK_PYTHON=$(which python3) \
PYSPARK_DRIVER_PYTHON=$(which python3) \
spark-submit \
  --master yarn \
  --deploy-mode client \
  --num-executors 1 \
  --executor-cores 1 \
  --executor-memory 1G \
  --driver-memory 1G \
spark.py
```

- Run with Dynamic Allocation

``` bash
PYSPARK_PYTHON=$(which python3) \
PYSPARK_DRIVER_PYTHON=$(which python3) \
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.shuffle.service.enabled=true \
  --conf spark.dynamicAllocation.minExecutors=1 \
  --conf spark.dynamicAllocation.maxExecutors=5 \
wordcount.py
```
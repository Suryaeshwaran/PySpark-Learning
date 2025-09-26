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
Details:
``` bash
--num-executors → total executors requested.
--executor-cores → CPU cores per executor.
--executor-memory → memory per executor (excluding overhead).
--driver-memory → memory for the driver.
```

👉 Example above requests:

- 3 executors × 2 cores = 6 cores total.
- 3 executors × 2G memory = 6G memory total.

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

In YARN:

- You can set queues (production, dev, etc.).
- Each queue can have resource limits.

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
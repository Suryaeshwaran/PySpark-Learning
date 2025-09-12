#### Day08: Understanding RDDs (Basics)
---
Before we start writing program lets understand the fundamental concept in Spark - RDD.

#### RDD stands for Resilient Distributed Dataset:
- These are the elements that run and operate on multiple nodes to do parallel processing on a cluster. 
- RDDs are immutable elements, which means once you create an RDD you cannot change it.
- RDDs are fault tolerant as well, hence in case of any failure, they recover automatically.
	- Resilient: Spark can recompute partitions using lineage if nodes fail.
	- You can apply multiple operations on these RDDs to achieve a certain task.

**Two operation types:**
To apply operations on these RDD's, there are two ways:-
- **Transformations (lazy):** map, filter, flatMap, reduceByKey, union, etc.
- **Actions (trigger computation):** collect, count, take, saveAsTextFile, etc.

**Transformation:**

- These are the operations, which are applied on a RDD to create a new RDD. 
- Filter, groupBy and map are the examples of transformations.

**Action:**

- These are the operations that are applied on RDD, which instructs Spark to perform computation and send the result back to the driver.

To apply any operation in PySpark, we need to create a PySpark RDD first. 

PySpark uses a **two-layer architecture:**
- **Python layer:** Provides the familiar Python API
- **JVM layer:** Handles the actual distributed processing

The RDD class acts as a bridge between these layers, allowing you to write Python code while leveraging Spark's distributed computing engine.

The following code block has the detail of a PySpark RDD Class 
- This is only a sample (Don't worry much about it now)
``` bash
class pyspark.RDD (
   jrdd, 
   ctx, 
   jrdd_deserializer = AutoBatchedSerializer(PickleSerializer())
)
```
#### Few important operations that are done on PySpark RDD:

**1. Count ()**

- Number of elements in the RDD is returned.
``` python
from pyspark import SparkContext

sc = SparkContext("local", "count app")

words = sc.parallelize (
   ["scala", 
   "java", 
   "hadoop", 
   "spark", 
   "akka",
   "spark vs hadoop", 
   "pyspark",
   "pyspark and spark"]
)

counts = words.count()
print ("Number of elements in RDD -> %i" % counts)
```
**Output:**
``` bash
Number of elements in RDD -> 8
```
**2. Collect ()**

- All the elements in the RDD are returned.
``` python
from pyspark import SparkContext
sc = SparkContext("local", "Collect app")
words = sc.parallelize (
   ["scala", 
   "java", 
   "hadoop", 
   "spark", 
   "akka",
   "spark vs hadoop", 
   "pyspark",
   "pyspark and spark"]
)
coll = words.collect()
print ("Elements in RDD -> %s" % coll)
```
**Output:**
``` bash
Elements in RDD -> ['scala', 'java', 'hadoop', 'spark', 'akka', 'spark vs hadoop', 'pyspark', 'pyspark and spark']
```
**3. foreach(f)**

- Returns only those elements which meet the condition of the function inside foreach. 
- In the following example, we call a print function in foreach, which prints all the elements in the RDD.
``` python
from pyspark import SparkContext
sc = SparkContext("local", "ForEach app")
words = sc.parallelize (
   ["scala", 
   "java", 
   "hadoop", 
   "spark", 
   "akka",
   "spark vs hadoop", 
   "pyspark",
   "pyspark and spark"]
)
def f(x):
     print ("Element is -> %s" % x)

words.foreach(f)
```
**Output:**
``` bash
Element is -> scala                                                 
Element is -> java
Element is -> hadoop
Element is -> spark
Element is -> akka
Element is -> spark vs hadoop
Element is -> pyspark
Element is -> pyspark and spark
```
**4. filter()**

- A new RDD is returned containing the elements, which satisfies the function inside the filter. 
- In the following example, we filter out the strings containing ''spark".
``` python
from pyspark import SparkContext
sc = SparkContext("local", "Filter app")
words = sc.parallelize (
   ["scala", 
   "java", 
   "hadoop", 
   "spark", 
   "akka",
   "spark vs hadoop", 
   "pyspark",
   "pyspark and spark"]
)
words_filter = words.filter(lambda x: 'spark' in x)
filtered = words_filter.collect()
print ("Fitered RDD -> %s" % filtered)
```
**Output:**
``` bash
Fitered RDD -> ['spark', 'spark vs hadoop', 'pyspark', 'pyspark and spark']
```
**5. map()**

- A new RDD is returned by applying a function to each element in the RDD. 
- In the following example, we form a key value pair and map every string with a value of 1.
``` python
from pyspark import SparkContext
sc = SparkContext("local", "Map app")
words = sc.parallelize (
   ["scala", 
   "java", 
   "hadoop", 
   "spark", 
   "akka",
   "spark vs hadoop", 
   "pyspark",
   "pyspark and spark"]
)
words_map = words.map(lambda x: (x, 1))
mapping = words_map.collect()
print ("Key value pair -> %s" % mapping)
```
**Output:**
``` bash
Key value pair -> [('scala', 1), ('java', 1), ('hadoop', 1), ('spark', 1), ('akka', 1), ('spark vs hadoop', 1), ('pyspark', 1), ('pyspark and spark', 1)]
```
**6. reduce()**

- After performing the specified commutative and associative binary operation, the element in the RDD is returned. 
- In the following example, we are importing add package from the operator and applying it on num to carry out a simple addition operation.
``` python
from pyspark import SparkContext
from operator import add

sc = SparkContext("local", "Reduce app")
nums = sc.parallelize([1, 2, 3, 4, 5])

adding = nums.reduce(add)

print ("Adding all the elements -> %i" % adding)
```
**Output:**
``` bash
Adding all the elements -> 15
```
**7. Join()**

- It returns RDD with a pair of elements with the matching keys and all the values for that particular key. 
- In the following example, there are two pair of elements in two different RDDs.
- After joining these two RDDs, we get an RDD with elements having matching keys and their values.
``` python
from pyspark import SparkContext

sc = SparkContext("local", "Join app")

x = sc.parallelize([("spark", 1), ("hadoop", 4), ("java", 5)])
y = sc.parallelize([("spark", 2), ("hadoop", 5), ("java", 6),])

joined = x.join(y)
final = joined.collect()

print ("Join RDD -> %s" % final)
```
**Output:**
``` bash
Join RDD -> [('hadoop', (4, 5)), ('java', (5, 6)), ('spark', (1, 2))]
```
**8. Cache()**

- Persist this RDD with the default storage level (MEMORY_ONLY). You can also check if the RDD is cached or not.
``` python
from pyspark import SparkContext 

sc = SparkContext("local", "Cache app") 

words = sc.parallelize (
   ["scala", 
   "java", 
   "hadoop", 
   "spark", 
   "akka",
   "spark vs hadoop", 
   "pyspark",
   "pyspark and spark"]
) 

words.cache() 
caching = words.persist().is_cached 

print ("Words got chached ? %s" % caching)
```
**Output:**
``` bash
Words got chached ? True
```
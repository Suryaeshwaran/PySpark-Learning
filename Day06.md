#### Day06:  Introduction to PySpark
---

#### What is PySpark?
- Apache Spark is a powerful open-source data processing engine written in Scala, designed for large-scale data processing.
- To support Python with Spark, Apache Spark community released a tool, PySpark. 
- Using PySpark, you can work with RDDs(Resilient Distributed Datasets) in Python programming language also.
- PySpark is the Python API for Apache Spark. It allows you to interact/interface with Spark's distributed computation framework using Python, making it easier to work with big data in a language many data scientists and engineers are familiar with. 
- By using PySpark, you can create and manage Spark jobs, and perform complex data transformations and analysis.

#### Why PySpark?
- The primary purpose of PySpark is to enable processing of large-scale datasets in real-time across a distributed computing environment using Python.
- PySpark provides an interface for interacting with Spark's core functionalities, such as working with Resilient Distributed Datasets (RDDs) and DataFrames, using the Python programming language.

#### Key Components of PySpark

- **RDDs (Resilient Distributed Datasets):**
	- RDDs are the fundamental data structure in Spark. 
	- They are immutable(unchangeable) distributed collections of objects that can be processed in parallel.
- **DataFrames:** 
	- DataFrames are similar to RDDs but with additional features like named columns, and support for a wide range of data sources. 
	- They are similar to tables in a relational database and provide a higher-level abstraction for data manipulation.
- **Spark SQL:** 
	- This module allows you to execute SQL queries on DataFrames and RDDs.
	- It provides a programming abstraction called DataFrame and can also act as a distributed SQL query engine.
- **MLlib (Machine Learning Library):** 
	- MLlib is Spark's scalable machine learning library, offering various algorithms and utilities for classification, regression, clustering, collaborative filtering, and more.
- **Spark Streaming:** 
	- Spark Streaming enables real-time data processing and stream processing. 
	- It allows you to process live data streams and update results in real-time.

####  Features of PySpark

- **Integration with Spark:−** 
	- PySpark is tightly integrated with Apache Spark, allowing seamless data processing and analysis using Python Programming.
- **Real-time Processing:−** 
	- It enables real-time processing of large-scale datasets.
- **Ease of Use:−** 
	- PySpark simplifies complex data processing tasks using Python's simple syntax and extensive libraries.
- **Interactive Shell:−** 
	- PySpark offers an interactive shell for real-time data analysis and experimentation.
- **Machine Learning:−** 
	- It includes MLlib, a scalable machine learning library.
- **Data Sources:−** 
	- PySpark can read data from various sources, including HDFS, S3, HBase, and more.
- **Partitioning:−** 
	- Efficiently partitions data to enhance processing speed and efficiency.

#### Applications of PySpark

PySpark is widely used in various applications, including −

- **Data Analysis−** Analysing large datasets to extract meaningful information.
- **Machine Learning−** Implementing machine learning algorithms for predictive analytics.
- **Data Streaming−** Processing streaming data in real-time.
- **Data Engineering−** Managing and transforming big data for various use cases.

#### Prerequisites of Concept to learn PySpark:

**Apache Hadoop**
- Open-source framework for storing and processing big data across clusters of computers
- Uses MapReduce programming model for distributed processing
- Provides fault tolerance and scalability for handling petabyte-scale datasets

**Scala Programming Language**
- Functional and object-oriented programming language that runs on the Java Virtual Machine
- Combines features of both paradigms with concise, expressive syntax
- Native language for Apache Spark development with strong type safety

**Hadoop Distributed File System (HDFS)**
- Distributed storage system that splits large files across multiple machines in a cluster
- Provides high fault tolerance through automatic data replication across nodes
- Optimized for high-throughput access to large datasets rather than low-latency operations

**Python**
- High-level, interpreted programming language known for simple, readable syntax
- Extensive ecosystem of libraries for data science, machine learning, and web development
- Popular choice for big data processing through frameworks like PySpark and pandas

**Apache Spark**
- In-memory distributed computing engine for fast processing of large datasets
- Provides unified APIs for batch processing, real-time streaming, machine learning, and graph processing
- Much faster than traditional MapReduce due to memory-based computations
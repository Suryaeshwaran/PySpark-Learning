#### 📘 Day07: Installing PySpark & Run Script
---

#### Below is a concise, copy-pasteable:
- step-by-step guide to install PySpark on your Mac (zsh), 
- create an isolated virtualenv, and run a small PySpark script locally.

#### 1) Quick checklist (what you need)
1. Python 3.9+ (Spark 4.x notes). 
2. Java JDK 17 (or later) and JAVA_HOME set.
3. pip (comes with Python) and (optionally) Homebrew to install Java/Python easily.

#### 2) Install Homebrew (only if you don't have it)
``` bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
# then (for a fresh mac) add brew to shell env (if brew tells you to):
echo 'eval "$(/opt/homebrew/bin/brew shellenv)"' >> ~/.zprofile
eval "$(/opt/homebrew/bin/brew shellenv)"
```
#### 3) Install Java 21 (recommended) via Homebrew
Either of these works; pick one.
Option A — OpenJDK 21:
``` bash
brew install openjdk@21
# Add JAVA_HOME permanently (zsh)
echo 'export JAVA_HOME=$(/usr/libexec/java_home -v 17)' >> ~/.zshrc
echo 'export PATH="$JAVA_HOME/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
```
#### 4) Create & activate a Python virtual environment
``` bash
python3 -m venv ~/myspark-venv
source ~/myspark-venv/bin/activate
python -m pip install --upgrade pip setuptools wheel
```
#### 5) Install PySpark with pip
``` bash
pip install pyspark
# optional extras:
# pip install "pyspark[sql]"         # if you want Spark SQL-specific extras
# pip install "pyspark[pandas_on_spark]"  # if you'll use pandas API on Spark
```

#### 6) Quick verify (from the venv)
``` python
python -c "import pyspark; print('pyspark', pyspark.__version__)"

# quick Spark session check:

python - <<'PY'
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").appName("verify").getOrCreate()
print("Spark version:", spark.version)
spark.stop()
PY

```

#### 7)  PySpark script (create a DataFrame & show)
**Example 1:**
- Create file *example_pyspark.py* in some folder (e.g. ~/pyspark-examples/):
``` python
from pyspark.sql import SparkSession

# 1) Create/obtain SparkSession (starts Spark in local mode)
spark = SparkSession.builder \
    .appName("Day7LocalTest") \
    .master("local[*]") \
    .getOrCreate()

# 2) Sample data in driver (small list)
data = [("Alice", 34), ("Bob", 36), ("Cathy", 30)]

# 3) Convert to Spark DataFrame (schema inferred)
df = spark.createDataFrame(data, ["name", "age"])

# 4) Action: show() triggers execution and prints rows
df.show()  # default: shows up to 20 rows, may truncate long columns

# 5) Transformation: groupBy + avg (lazy), then action: collect() to get result
avg_age = df.groupBy().avg("age").first()[0]  # returns a Python float

# 6) Print and stop Spark
print(f"Average age = {avg_age}")
spark.stop()
```
**Example 2: PySpark script - Read data from csv file**
- Create file *sample-csv.py* in some folder keep *file.csv*
``` python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("hello").getOrCreate()
df = spark.read.csv("file.csv", header=True, inferSchema=True)
df.show()
```
**Example 3: PySpark script - Read data from json file**
- Create file *read_json_example.py* in some folder keep *people.json*
``` python
from pyspark.sql import SparkSession

# Start Spark session
spark = SparkSession.builder \
    .appName("ReadJSONExample") \
    .master("local[*]") \
    .getOrCreate()

# Read JSON file into DataFrame
df = spark.read.json("people.json")

# Show schema (structure of the JSON)
df.printSchema()

# Show first few rows
df.show()

# Example: Select only name and city columns
df.select("name", "city").show()

# Example: Filter people older than 32
df.filter(df.age > 32).show()

spark.stop()
```
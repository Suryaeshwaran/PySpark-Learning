#### Day03:  Python Basics (DataFrames, Filtering)
---
**Pandas** is a popular **open-source Python library** used for **data manipulation and analysis.**
- It provides fast, flexible, and expressive data structures designed to make working with structured data both easy and intuitive. 
- It's a fundamental tool for data scientists and analysts.

**Key Features of Pandas**
- **Data Structures:** Pandas' primary data structures are the Series (a one-dimensional labeled array) and the DataFrame (a two-dimensional labeled data structure, like a spreadsheet or SQL table).
- **Data Cleaning and Preparation:** It offers powerful tools to handle missing data, merge and join datasets, and reshape and pivot data.
- **Data Analysis:** You can use it to perform group-by operations, aggregations, time-series analysis, and statistical computations.
- **Input/Output:** Pandas can read and write data from various formats, including CSV, Excel, SQL databases, and HDF5.
- **Integration:** It works seamlessly with other Python libraries like NumPy for numerical operations and Matplotlib for data visualization.
#### 1. Pandas DataFrame
_**What is a Pandas DataFrame?**_
- A **DataFrame** is a 2 dimensional data structure, like a 2 dimensional array, like an Excel sheet(rows x columns) stored in Python.
- It's built on top of NumPy arrays, but adds labels(rows x columns)  for easier handling.

**Pandas Installation & Calling in Program:**
``` bash
# Setup
pip install pandas

# In Python Program
import pandas as pd
```
**Creating a DataFrame from a Dictionary:**
``` python
import pandas as pd
data = {
    'Name': ['Alice', 'Bob', 'Charlie'],
    'Age': [25, 30, 35],
    'City': ['New York', 'Los Angeles', 'Chicago']
}

#load data into a DataFrame object:
df = pd.DataFrame(data)
print("Printing DataFrame from a Dictionary:")
print(df)
```
**Create a DataFrame from list of lists:**
``` python
import pandas as pd
data = [
    ['Suresh', 28, 'Bangalore'],
    ['Ravi', 34, 'Chennai'],
    ['Anita', 29, 'Mumbai']
]
# loading data with Custom Columns
df = pd.DataFrame(data, columns=['Name', 'Age', 'City'])
print("Printing DataFrame from a List of Lists:")
print(df)
```
**Example of setting a custom index in a DataFrame**
``` python
import pandas as pd
data = {
    'Steps': [3000, 2000, 3500],
    'HeartPts': [30, 25, 35]
}
# Load data with custom index
df = pd.DataFrame(data, index=['Day1', 'Day2', 'Day3'])
print("Printing DataFrame with Custom Index:")
print(df)
```
**Locate Named Indexes**
``` python
#refer to the named index:
print(df.loc["Day2"])
```
**Load Files into a DataFrame**
- We can load data from csv files into DataFrame using read_csv() 
``` python
# Load data from csv into DataFrame
import pandas as pd

#'mydata.csv' is the CSV file in the current directory
df = pd.read_csv('mydata.csv')
print(df)
```
- load data from json file into DataFrame using read_json()
``` python
import pandas as pd
df = pd.read_json('products.json') 
print(df.to_string) # to print whole string
```

#### 2. Filtering filter()
- Filter the DataFrame according to the specified filter
- The filter() method filters the DataFrame, and returns only the rows or columns that are specified in the filter.

**Syntax**
``` python
dataframe.filter(items, like, regex, axis)
```
Breakdown of each parameter in the syntax:
- **items:** This parameter takes a list of exact labels you want to keep. It's for when you know the precise names of the columns or index labels you're looking for. The names must match exactly.
- **like:** This parameter takes a string or a list of strings. It filters for labels that contain the specified string as a substring. It's a simple, non-regex search.
- **regex:** This parameter takes a string representing a regular expression. It's used for more advanced, pattern-based searches on your labels. It offers the most flexibility for matching.
- **axis:** This parameter specifies which axis to filter on. It can be 0 or 'index' to filter rows by their index labels, or 1 or 'columns' to filter columns by their column names. If you don't specify this, it defaults to filtering columns.

You should use only one of the items, like, or regex parameters at a time. Using more than one will result in a TypeError.

A DataFrame with the filtered result. This method does not change the original DataFrame.

**Practical Example**
``` python
import pandas as pd
data = {
    'Name': ['Alice', 'Bob', 'Charlie'],
    'Age': [25, 30, 35],
    'City': ['New York', 'Los Angeles', 'Chicago']
}
df = pd.DataFrame(data)

# Filter columns by specifying items
filtered_df_items = df.filter(items=['Name', 'City'])
print("Filtered DataFrame by items (Name and City):")
print(filtered_df_items)

# Filter columns by specifying a substring
filtered_df_like = df.filter(like='a', axis=1)
print("\nFiltered DataFrame by like (columns containing 'a'):")
print(filtered_df_like)

# Filter columns by specifying a regex pattern
filtered_df_regex = df.filter(regex='^A|C', axis=1)
print("\nFiltered DataFrame by regex (columns starting with 'A' or 'C'):")
print(filtered_df_regex)
```

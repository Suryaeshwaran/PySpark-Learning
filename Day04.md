#### Day04:  Learn Pandas (Grouping, Joins/Merge)
---
#### 1. Pandas Grouping
**Grouping** in Pandas is like: "Split the data into groups based on one or more columns, then apply an operation like sum, mean, count, etc. and then combine as result".
``` bash
Split -> Apply -> Combine
```
**Example Dataset**
``` python
import pandas as pd

# Example dataset
data = {
    'Department': ['HR', 'HR', 'IT', 'IT', 'Finance', 'Finance', 'Finance'],
    'Employee': ['John', 'Anna', 'Steve', 'Sara', 'Mike', 'Bob', 'Linda'],
    'Salary': [4000, 4500, 5000, 5200, 6000, 6200, 6100],
    'Bonus': [400, 500, 600, 550, 800, 750, 700]
}

df = pd.DataFrame(data)
print(df)
```
**Basic Grouping**
- Group the employees by Department wise
``` python
# Grouping by Department
grouped = df.groupby('Department')
print("\nGrouped DataFrame by Department:")
for name, group in grouped:
    print(f"\nDepartment: {name}")
    print(group)
```
- Group by one column (e.g., Department) and take the max:
``` python
# Group by Department and calculate max salary and bonus
grouped = df.groupby('Department')
max_values = grouped[['Salary', 'Bonus']].max()
print("\nmax Salary and Bonus by Department:")
print(max_values)
```
**Grouping with Multiple Columns**
- Groups by Department first, then Employee.
- Just the Output Result will print Department wise Employees list with Salary
``` python
# Group by Department and calculate the sum salary
grouped = df.groupby(['Department', 'Employee'])['Salary'].sum()
print("\nGrouped DataFrame (Sum of Salaries by Department and Employee):")
print(grouped)
```
**Multiple Aggregations:** agg() 
- Built-in functions: sum(), mean(), count(), min(), max()

- Shows multiple calculations per group.
``` python
# Grouping multiple calculations per group
grouped = df.groupby('Department').agg({
    'Salary': ['mean','max', 'min'],
    'Bonus': ['sum']
})
print("\nGrouped DataFrame with multiple aggregations:")
print(grouped)
```
**Accessing Groups:** get_group()
- Get only Finance department rows.
``` python
grouped = df.groupby('Department')
print(grouped.get_group('Finance'))

# Get only the Finance department rows
grouped = df.groupby('Department')
print("\nFinance Department Data:")
print(grouped.get_group('Finance'))
```
**Transform vs Apply**

Transform returns same-shaped data as original groups, while Apply can return aggregated results or different shapes - transform preserves group structure, apply is flexible.

- transform keeps the original shape (good for creating new columns).
- apply can reduce or change the shape.
``` python
# Transform example: Add a column with average salary per department
grouped = df.groupby('Department')
df['Avg_Salary_Department'] = grouped['Salary'].transform('mean')
print("\nDataFrame with Average Salary per Department:")
print(df)

# Apply example: Get the mean of Salary and Bonus per department
grouped = df.groupby('Department')
mean_values = grouped[['Salary', 'Bonus']].apply(lambda x: x.mean())
print("\nMean Salary and Bonus per Department:")
print(mean_values)

```
**Filtering Groups**
- Get the Department with average salary > 5000
``` python
# Departments where avg salary > 5000
grouped = df.groupby('Department').filter(lambda x: x['Salary'].mean() > 5000)
print("\nDepartments with average salary > 5000:")
print(grouped)
```
🚀 **Exercises:**

Try these after running the examples:
- Find the total salary and total bonus for each department.
- Get the highest paid employee in each department.
- Add a new column Salary_to_Bonus_Ratio grouped by Department.
- Filter only departments where sum of bonus > 1500.
- Create a pivot table from this dataset using department vs employee salary.
``` python
# Total Salary and Bonus by Department
grouped = df.groupby('Department').agg({'Salary': 'sum', 'Bonus': 'sum'})
print("\nTotal Salary and Bonus by Department:")
print(grouped)

# Get highest salary in each department with employee names
idx = df.groupby('Department')['Salary'].idxmax()
highest_salary = df.loc[idx, ['Department', 'Employee', 'Salary']]
print("\nHighest Salary in each Department:")
print(highest_salary)

#Add a new column Salary_to_Bonus_Ratio grouped by Department.
df['Salary_to_Bonus_Ratio'] = df['Salary'] / df['Bonus']
print("\nDataFrame with Salary to Bonus Ratio:")
print(df)

# Filter only departments where sum of bonus is greater than 1500
filtered = grouped[grouped['Bonus'] > 1500]
print("\nDepartments with Total Bonus greater than 1500:")
print(filtered)

# Create a pivot table from this dataset using department vs employee salary.
pivot_table = df.pivot_table(values='Salary', index='Department', columns='Employee', fill_value=0)
print("\nPivot Table of Department vs Employee Salary:")
print(pivot_table)
```

### 2. Pandas Joins/Merge
Joining DataFrames in Pandas is similar to joining tables in SQL. 

It's how you combine two or more DataFrames based on common columns, also known as keys. 

The main function you'll use is pd.merge(), which offers different types of joins.

**2.1. Basic Join Concepts:**

**Understanding Join Types**

The type of join determines which rows are included in the final DataFrame.

_Here are the four main types of joins:_
- **Inner Join:** This is the default. It returns only the rows where the join key exists in both DataFrames. Rows with keys that are only in one DataFrame are dropped.
- **Left Join:** Returns all rows from the left DataFrame and the matched rows from the right DataFrame. If a key from the left DataFrame doesn't have a match in the right DataFrame, the columns from the right DataFrame will have NaN (Not a Number) values.
- **Right Join:** The opposite of a left join. It returns all rows from the right DataFrame and the matched rows from the left. Unmatched rows from the left DataFrame will have NaN values.
- **Outer Join:** Returns all rows from both DataFrames. If a join key is missing in one of the DataFrames, the corresponding values in the new DataFrame will be NaN

**2.2 The pd.merge() Function**
- The pd.merge() function is your primary tool for joining. 

Here's its basic syntax and a breakdown of its key arguments:
``` python
pd.merge(left_df, right_df, on='key_column', how='join_type')
```
- left_df: The first DataFrame you're joining.
- right_df: The second DataFrame.
- on: The column name(s) to join on. This must be present in both DataFrames.
- how: The type of join you want to perform. The options are 'inner', 'left', 'right', or 'outer'.

**Practical Example:**

Let's imagine you have two DataFrames: one with employee IDs and their names (employees), and another with employee IDs and their salary information (salaries).
**DataFrame 1: employees**
``` python
   employee_id   name
0          101    Alice
1          102      Bob
2          103    Charlie

```
**DataFrame 2: salaries**
``` python
   employee_id  salary
0          101  60000
1          102  75000
2          104  90000

```
Notice that employee 103 is in employees but not salaries, and employee 104 is in salaries but not employees.
**Complete Code:**
``` python
# Example for join and merge operations in pandas
import pandas as pd

# Create sample DataFrames employees(employee_id, name) & salaries(employee_id, salary)
employees = pd.DataFrame({
    'employee_id': [101, 102, 103],
    'name': ['Alice', 'Bob', 'Charlie']
})
salaries = pd.DataFrame({
    'employee_id': [101, 102, 104],
    'salary': [70000, 80000, 90000]
})  

# Inner Join
inner_join = pd.merge(employees, salaries, on='employee_id', how='inner')
print("Inner Join:\n", inner_join)

# Left Join
left_join = pd.merge(employees, salaries, on='employee_id', how='left')
print("\nLeft Join:\n", left_join)

# Right Join
right_join = pd.merge(employees, salaries, on='employee_id', how='right')
print("\nRight Join:\n", right_join) 

# Outer Join
outer_join = pd.merge(employees, salaries, on='employee_id', how='outer')
print("\nOuter Join:\n", outer_join)
```
**Output:**
``` sql
Inner Join:
    employee_id   name  salary
0          101  Alice   70000
1          102    Bob   80000

Left Join:
    employee_id     name   salary
0          101    Alice  70000.0
1          102      Bob  80000.0
2          103  Charlie      NaN

Right Join:
    employee_id   name  salary
0          101  Alice   70000
1          102    Bob   80000
2          104    NaN   90000

Outer Join:
    employee_id     name   salary
0          101    Alice  70000.0
1          102      Bob  80000.0
2          103  Charlie      NaN
3          104      NaN  90000.0
```
**Other Useful Joins**

**Joining on different column names:**
- If the join columns have different names in each DataFrame, use left_on and right_on:
``` python
pd.merge(df1, df2, left_on='key_a', right_on='key_b')
```
**2.3. Joining on the index:**

**The .join() method:**
- df.join() is a convenient way to perform a left join on the index.

**How it Works?**

The df.join() method is a convenience method that is most commonly used for a left join on the index. Unlike pd.merge(), you call it directly on one of the DataFrames.

**Syntax**
``` python
left_df.join(right_df, on='key_column', how='join_type')
```
**Breakdown:**
``` text
left_df: The DataFrame on which the .join() method is called. This is the "left" DataFrame.
right_df: The DataFrame to join with.
on: The column in the left_df to join on. The right_df's index will be matched against this column. If you omit this, join defaults to using the index of both DataFrames.
how: The type of join to perform. The options are 'left' (default), 'right', 'inner', or 'outer'.
```
**The Most Common Use Case: Joining on the Index**

This is the primary use of df.join() because it's so concise. When you don't specify the on parameter, Pandas assumes you want to join the 
two DataFrames based on their indexes.

**Example: Joining Two DataFrames on their Indexes**

**Code:**
``` python
import pandas as pd

sales_data = {'revenue': [1000, 1500, 2000]}
sales = pd.DataFrame(sales_data, index=[101, 102, 103])
print("Sales DataFrame:")
print(sales)

products_data = {'product_name': ['Laptop', 'Keyboard', 'Mouse']}
products = pd.DataFrame(products_data, index=[101, 102, 104])
print("\nProducts DataFrame:")
print(products)

# Perform a left join on the index
joined_df = sales.join(products)
print("\nLeft Joined DataFrame:")
print(joined_df)

# Perform an outer join on the index
outer_joined_df = sales.join(products, how='outer')
print("\nOuter Joined DataFrame:")
print(outer_joined_df)
```
**Output:**
``` sql
Sales DataFrame:
     revenue
101     1000
102     1500
103     2000

Products DataFrame:
    product_name
101       Laptop
102     Keyboard
104        Mouse

Left Joined DataFrame:
     revenue product_name
101     1000       Laptop
102     1500     Keyboard
103     2000          NaN

Outer Joined DataFrame:
     revenue product_name
101   1000.0       Laptop
102   1500.0     Keyboard
103   2000.0          NaN
104      NaN        Mouse
```
**Pro-Tip:**
- Use df.join() when you want to join on the index, as it is more concise.
- For more complex joins involving columns or multiple keys, pd.merge() is the standard and more explicit tool.
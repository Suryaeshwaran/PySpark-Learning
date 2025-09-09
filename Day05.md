#### Day05:  Brush up SQL (SELECT, WHERE, GROUP BY, JOIN)
---
SQL -> Structured Query Language, 
- SQL lets you access and manipulate data in relational databases.
- SQL statements consist of keywords that are easy to understand.

#### 1. SELECT

- The SELECT statement is used to select data from a database.
The following SQL statement returns all records from a table named "Customers":

SQL Try it: [For W3 School SQL Try it:] [https://www.w3schools.com/sql/trysql.asp?filename=trysql_select_all]

``` sql
SELECT * FROM Customers;
```
- This is show all records from the Customers tables, like wise we can choose other tables as well.
**Example:**
``` sql
SELECT CustomerName,City FROM Customers;
```
- The SELECT DISTINCT statement is used to return only distinct (different) values.
A column often contains many duplicate/repeated values, to list distinct values we can use this select option.
``` sql
SELECT DISTINCT Country FROM Customers;
```

#### 2. WHERE

- The WHERE clause is used to filter records.
- It is used to extract only those records that fulfill a specified condition.

**Example:**

String has to be in single quotes, numeric fields without quotes.
``` sql
SELECT * FROM Customers WHERE Country='Mexico';
select * from customers where country='mexio'; # Also Works

SELECT * FROM Customers WHERE CustomerID=1;
```
- Operators in WHERE clause 
```sql
=, >, <,< > or ! = ,Greater than or equal, Less than or equal
BETWEEN,LIKE,IN 
```
**Example:**
``` sql
SELECT * FROM Customers WHERE CustomerID > 80;
```
#### 3. GROUP BY

- The GROUP BY statement groups rows that have the same values into summary rows, 
- like "find the number of customers in each country".

**Example:**

The following SQL statement lists the number of customers in each country:
``` sql
SELECT COUNT(CustomerID), Country
FROM Customers
GROUP BY Country;
```
The following SQL statement lists the number of customers in each country, sorted high to low:
``` sql
SELECT COUNT(CustomerID), Country
FROM Customers
GROUP BY Country
ORDER BY COUNT(CustomerID) DESC;
``` 
**GROUP BY with JOIN EXAMPLE**

The following SQL statement lists the number of orders sent by each shipper:
``` sql
SELECT Shippers.ShipperName, COUNT(Orders.OrderID) AS NumberOfOrders FROM Orders
LEFT JOIN Shippers ON Orders.ShipperID = Shippers.ShipperID
GROUP BY ShipperName;
```
#### 4. JOIN

A JOIN clause is used to combine rows from two or more tables, based on a related column between them.

Let's look at the "Orders" & "Customers" table:
- The relationship between the two tables above is the "CustomerID" column
Then, we can create the following SQL statement (that contains an INNER JOIN), 
that selects records that have matching values in both tables:
``` sql
SELECT Orders.OrderID, Customers.CustomerName, Orders.OrderDate
FROM Orders
INNER JOIN Customers ON Orders.CustomerID=Customers.CustomerID;
```
*Here are the different types of the JOINs in SQL:*
- (INNER) JOIN: Returns records that have matching values in both tables
- LEFT (OUTER) JOIN: Returns all records from the left table, and the matched records from the right table
- RIGHT (OUTER) JOIN: Returns all records from the right table, and the matched records from the left table
- FULL (OUTER) JOIN: Returns all records when there is a match in either left or right table

**4.1. INNER JOIN**

The INNER JOIN keyword selects records that have matching values in both tables.

Let's look at the "Products" & "Categories" tables
- The relationship between the two tables above is the "CategoryID" column
- We will join the Products table with the Categories table, by using the CategoryID field from both tables.

**Example**
``` sql
SELECT ProductID, ProductName, CategoryName
FROM Products
INNER JOIN Categories ON Products.CategoryID = Categories.CategoryID;
```
It is a good practice to include the table Name when specifying columns in the SQL statement.
``` sql
SELECT Products.ProductID, Products.ProductName, Categories.CategoryName
FROM Products
INNER JOIN Categories ON Products.CategoryID = Categories.CategoryID;
```

JOIN and INNER JOIN will return the same result.
INNER is the default join type for JOIN, so when you write JOIN the parser actually writes INNER JOIN.

**JOIN Three Tables**

**Example:**
``` sql
SELECT Orders.OrderID, Customers.CustomerName, Shippers.ShipperName
FROM ((Orders INNER JOIN Customers ON Orders.CustomerID = Customers.CustomerID)
INNER JOIN Shippers ON Orders.ShipperID = Shippers.ShipperID);
```

**4.2. LEFT JOIN**

The LEFT JOIN keyword returns all records from the left table (table1), and the matching records from the right table (table2). 
- The result is 0 records from the right side, if there is no match.

**Syntax**
``` sql
SELECT column_name(s)
FROM table1
LEFT JOIN table2
ON table1.column_name = table2.column_name;
```
**Example:**
- B/W Customers & Orders table, the following SQL statement will select all customers, and any orders they might have:
``` sql
SELECT Customers.CustomerName, Orders.OrderID
FROM Customers
LEFT JOIN Orders ON Customers.CustomerID = Orders.CustomerID
ORDER BY Customers.CustomerName;
```
Note: The LEFT JOIN keyword returns all records from the left table (Customers), even if there are no matches in the right table (Orders).

**4.3. Right JOIN**

The RIGHT JOIN keyword returns all records from the right table (table2), and the matching records from the left table (table1). 
- The result is 0 records from the left side, if there is no match.

**Syntax**
``` sql
SELECT column_name(s)
FROM table1
RIGHT JOIN table2
ON table1.column_name = table2.column_name;
```
**Example:**

Note: In some databases RIGHT JOIN is called RIGHT OUTER JOIN

- From the tables "Orders" & "Employees" return all employees, and any orders they might have placed:

``` sql
SELECT Orders.OrderID, Employees.LastName, Employees.FirstName
FROM Orders
RIGHT JOIN Employees ON Orders.EmployeeID = Employees.EmployeeID
ORDER BY Orders.OrderID;
```

Note: The RIGHT JOIN keyword returns all records from the right table (Employees), even if there are no matches in the left table (Orders).

**4.4. Full JOIN**

The FULL OUTER JOIN keyword returns all records when there is a match in left (table1) or right (table2) table records.

Tip: FULL OUTER JOIN and FULL JOIN are the same.

**Syntax**
``` sql
SELECT column_name(s)
FROM table1
FULL OUTER JOIN table2
ON table1.column_name = table2.column_name
WHERE condition;
```
Note: FULL OUTER JOIN can potentially return very large result-sets!

**Example**

From the tables "Customers" & "Orders", selects all customers, and all orders:
``` sql
SELECT Customers.CustomerName, Orders.OrderID
FROM Customers
FULL OUTER JOIN Orders ON Customers.CustomerID=Orders.CustomerID
ORDER BY Customers.CustomerName;
```
**4.x. Self Join**
- A self join is a regular join, but the table is joined with itself.

**Syntax**
``` sql
SELECT column_name(s)
FROM table1 T1, table1 T2
WHERE condition;
```
**Example:**

From the tables "Customers", matches customers that are from the same city:
``` sql
SELECT A.CustomerName AS CustomerName1, B.CustomerName AS CustomerName2, A.City
FROM Customers A, Customers B
WHERE A.CustomerID <> B.CustomerID
AND A.City = B.City
ORDER BY A.City;
```
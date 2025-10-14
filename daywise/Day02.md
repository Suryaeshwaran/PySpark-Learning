#### 📘 Day02:  Python Basics (Functions, File handling)
---
#### 1. Functions
- A function is a block of code which runs only when it is called.
- You can pass data, know as parameters into a function and a function can return data as result.

In Python a function is defined using the *'def'* keyword:

``` python
def my_function():
    print("This is my Test Function")
```
**Calling a function**
``` python
def my_function():
    print("This is my Test Function")

# calling a function
my_function()
```
**Arguments in function**
- Arguments are specified after the function name, inside the parentheses. 
``` python
def my_funtion1(fname):
    print(fname + " Kumar")   

my_funtion1("Suresh")
my_funtion1("Rajesh")
my_funtion1("Senthil")
```
**Function with 2 args**
- You can add as many arguments as you want, just separate them with a comma.
``` python
def my_funtion2(fname, lname):
    print(fname + " " + lname)  
    
print("\nFunction with 2 parameters")
my_funtion2("Raj", "Kumar")
```
**Arbitrary Argument** (*args) 
- When we don't know how many arguments will be passed, we can add a  *  before the parameter name in the function definition.
- This way the function will receive a tuple(a data structure consists of multiple parts) of arguments, and can access the items accordingly:
``` python
def my_funtion3(*kids):
    print("\nThe youngest child is " + kids[1])

my_funtion3("Emil", "Tobias", "Linus")

my_funtion3("Suresh", "Rajesh", "Senthil", "Kumar")
```
**Keyword argument**
- We can also send arguments with the _key = value_ syntax.
- This way the order of the arguments does not matter.
``` python
def my_funtion4(child3, child2, child1):
    print("\nThe youngest child is " + child3)

my_funtion4(child1 = "Akila", child2 = "Vini", child3 = "Surya")
```
**Arbitrary Keyword argument** (**kwargs)
- When we do not know how many keyword arguments that will be passed into the function, add two asterisk: ** before the parameter name in the function definition.
``` python
def my_funtion5(**kid):
    print("\nHis first name is " + kid["fname"])
    print("\nHis last name is " + kid["lname"])

my_funtion5(fname = "Mohan", lname = "Doss")
```
**Default parameter value**
- We can set a default value for argument when no param is passed.
``` python
def my_funtion6(country = "India"):
    print("\nI am from " + country) 

my_funtion6("Sweden")
my_funtion6()
```
**Passing a list as an argument**
- We can send any data types of argument to a function (string, number, list, dictionary etc.), and it will be treated as the same data type inside the function.
``` python
def my_funtion7(foods):
    for x in foods:
        print(x)    

foods = ["idly", "dosa", "poori"]
my_funtion7(foods)
```
**Return Values**
- To let a function return a value, use the return statement:
```python
def my_funtion8(x):
    return 5 * x    

print("\nFunction with return values")
print(my_funtion8(3))
```
The *pass* statement
- Function definitions cannot be empty, but if you for some reason have a function definition with no content, put in the pass statement to avoid getting an error.
``` python 
def my_funtion9():
    pass
```
**Position-Only Argument**
- To make an argument positional-only, use the forward slash (/)  symbol. All the arguments before this symbol will be treated as positional-only.
- Everything before (/) must be passed by position, not as keywords.

*Why ?*
- To prevent users from relying on parameter names (so you can change names later without breaking code).
- To make functions shorter and cleaner when names don’t add meaning.

✅ **Example use cases:**
- Math functions (pow(x, y)) → arguments are obvious by position.
- Built-in functions (like len(obj)) → no keyword needed.
``` python
def my_funtion10(name, /, age):
    print("\nMy name is " + name)
    print("My age is " + str(age))

my_funtion10("Suresh", 25) # ✅ works
my_funtion10(name="Suresh", age=25) # ❌ error (because of `/`)
```
**Keyword-Only Argument**
- To specify that a function can have only keyword arguments, add ** , before the arguments:
- Everything after **  must be passed as a keyword argument.

*Why ?*
- To make code more readable and avoid confusion when a function has many optional arguments.
- To force clarity when an argument’s meaning is not obvious by position.

✅ **Example use cases:**
- Functions with many optional settings (like plotting libraries, file operations, APIs).
- Makes it clear what each value represents.
``` python
def my_funtion11(*, name, age):
    print("\nMy name is " + name)
    print("My age is " + str(age))  

my_funtion11(name="Rajesh", age=30)    # ✅ works
my_funtion11("Suresh", 30) # ❌ error (because of `*`)
```
**Combine Positional-Only and Keyword-Only**
``` python
def func(a, b, /, c, *, d, e):
    print(a, b, c, d, e)

func(1, 2, 3, d=4, e=5)   # ✅
```
**Recursion**
- Recursion means a function calling itself.
- In programming, recursion is often used for problems that can be broken down into smaller versions of the same problem.

🔹 **Important parts of recursion**
- Base case → The condition that stops the recursion (otherwise it will go on forever).
- Recursive case → The part where the function calls itself with a smaller/simpler input.

✅ **Example use cases:**
- Countdown, sorting, searching.
- factorials, Fibonacci, tree traversal.

**Example: Countdown**
``` python
def countdown(n):
    if n == 0:   # Base case
        print("Blast off!")
    else:        # Recursive case
        print(n)
        countdown(n-1)

countdown(5)
```
**Example: Sum of numbers**
``` python
def sum_natural(n):
    if n == 1:    # Base case
        return 1
    else:
        return n + sum_natural(n-1)

print("\nSum of first 5 natural numbers is", sum_natural(5))   # 5 + 4 + 3 + 2 + 1 = 15
```
#### 2. File Handling
File handling means working with files on your computer using Python:
- Python has several functions to Create , Open, Read, Write and Close files.
The key function for working with files in Python is the open() function
- The open() function takes two parameters; filename, and mode.
There are four different methods (modes) for opening a file:
``` bash
"r" - Read - Default value. Opens a file for reading, error if the file does not exist
"a" - Append - Opens a file for appending, creates the file if it does not exist
"w" - Write - Opens a file for writing, creates the file if it does not exist
"x" - Create - Creates the specified file, returns an error if the file exists

# In addition you can specify if the file should be handled as binary or text mode

"t" - Text - Default value. Text mode
"b" - Binary - Binary mode (e.g. images)
```
**Syntax**
``` python
# To open a file for reading it is enough to specify the name of the file:
file = open("myfile.txt")

#The code above is the same as:
file = open("myfile.txt", "rt")

# Because "r" for read, and "t" for text are the default values, you do not need to specify them.

# 👉 Always close the file when done:
file.close()
```
Open a File on the Server
Make the the file has some content inside.
``` python
file = open("example.txt")
print(file.read())
file.close()
```
Using _**with**_ (Best Practice ✅)
- Instead of open() and close(), use with.  
- Python will automatically close the file when done.
``` python
with open("example.txt") as file:
    print(file.read())
```
**Read Lines**
``` python
# Read one line of the file:
  print(file.readline())
# Read two lines of the file:
  print(file.readline())
  print(file.readline())
```
**Writing to a File**
``` python
file = open("myfile.txt", "w")
file.write("Hello, World!")
file.write("\nThis is my first file in Python!\n")
file.close()
print("myfile.txt File written successfully.")
```
**Write to an Existing file**

To write to an existing file, you must add "a" parameter to the open() function:
``` python
file = open("myfile.txt", "a")
file.write("This is my second line.\n")
```
**Overwrite Existing Content**
``` python
# Overwriting the file
with open("example.txt", "w") as file:
    file.write("Oops! I have deleted the content! - overwrite \n")
print("example.txt File overwritten successfully.")

#open and read the file after the overwriting:
with open("example.txt") as f:
  print(f.read())
```
**Writing List of Lines**
``` python
# Write list of lines to a file
lines = ["First line.\n", "Second line.\n", "Third line.\n"]
with open("lines.txt", "w") as file:
    file.writelines(lines)
print("lines.txt File with multiple lines written successfully.")
```
**Example with "x" mode**

"x" - Create - will create a file, returns an error if the file exists
``` python
try:
    with open("newfile.txt", "x") as file:
        file.write("This file is created using x mode.\n")
    print("newfile.txt File created successfully.")
    # Trying to create the same file again will raise an error
    with open("newfile.txt", "x") as file:
        file.write("This will raise an error.\n")
except FileExistsError:
    print("newfile.txt already exists. Cannot create the file again using x mode.")
```
**Delete a File**
``` python
import os
os.remove("newfile.txt")
print("newfile.txt File deleted successfully.")
```
**Check if File exist then delete:**
``` python
import os
if os.path.exists("example.txt"):
    os.remove("example.txt")
    print("example.txt File deleted successfully.")
else:
    print("The file does not exist.")
```
**Delete a folder**

Note: You can only remove empty folders.
``` python
import os
folder = "myfolder"
if os.path.exists(folder) and os.path.isdir(folder):
    os.rmdir(folder)
    print(f"{folder} Folder deleted successfully.")
else:
    print("The folder does not exist.")
```
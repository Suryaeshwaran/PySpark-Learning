#### Day02:  Python Basics (Functions, File handling)
---
#### 1. Functions
- A function is a block of code which runs only when it is called.
- You can pass data, know as parameters into a function and a function can return data as result.

In Python a function is defined using the <span style="color:lightgreen">'def'</span> keyword:

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
**Arbitrary Argument** <span style="color:lightgreen">(*args) </span>
- When we don't know how many arguments will be passed, we can add a <span style="color:lightgreen"> * </span> before the parameter name in the function definition.
- This way the function will receive a tuple(a data structure consists of multiple parts) of arguments, and can access the items accordingly:
``` python
def my_funtion3(*kids):
    print("\nThe youngest child is " + kids[1])

my_funtion3("Emil", "Tobias", "Linus")

my_funtion3("Suresh", "Rajesh", "Senthil", "Kumar")
```
**Keyword argument**
- We can also send arguments with the <span style="color:lightgreen">key = value </span>syntax.
- This way the order of the arguments does not matter.
``` python
def my_funtion4(child3, child2, child1):
    print("\nThe youngest child is " + child3)

my_funtion4(child1 = "Akila", child2 = "Vini", child3 = "Surya")
```
**Arbitrary Keyword argument** <span style="color:lightgreen">(**kwargs)</span>
- When we do not know how many keyword arguments that will be passed into the function, add two asterisk: <span style="color:lightgreen">**</span> before the parameter name in the function definition.
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
The <span style="color:lightgreen">pass </span> statement
- Function definitions cannot be empty, but if you for some reason have a function definition with no content, put in the pass statement to avoid getting an error.
``` python 
def my_funtion9():
    pass
```
**Position-Only Argument**
- To make an argument positional-only, use the forward slash <span style="color:lightgreen">(/) </span> symbol. All the arguments before this symbol will be treated as positional-only.
- Everything before <span style="color:lightgreen">/</span> must be passed by position, not as keywords.

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
- To specify that a function can have only keyword arguments, add <span style="color:lightgreen">*</span>, before the arguments:
- Everything after <span style="color:lightgreen">* </span> must be passed as a keyword argument.

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
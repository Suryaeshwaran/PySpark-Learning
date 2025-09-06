- #### Day01:  Python Basics (Lists, Dictionary, Loops)
---
1. List
2. Dictionary
3. For Loop
There are four collection data types in the Python programming language:
- List is a collection which is ordered and changeable. Allows duplicate members.
- Tuple is a collection which is ordered and unchangeable. Allows duplicate members.
- Set is a collection which is unordered, unchangeable(but can remove or add), and unindexed. No duplicate members.
- Dictionary is a collection which is ordered** and changeable. No duplicate members.
- [ ] Lists
	- Lists are used to store multiple items in a single variable.
	- Lists are one of 4 built-in data types in Python used to store collections of data, the other 3 are Tuple, Set, and Dictionary, all with different qualities and usage.
	- Lists are created using square brackets: (list.py)

```
mylist = ["apple", "banana", "cherry"]
print(mylist)
```
List items:
- List items are ordered, changeable, and allow duplicate values.
- List items are indexed, the first item has index [0], the second item has index [1] etc.

```
print(mylist[0])

# To see the type
print(type(mylist))
```

- [ ] Dictionary
	- Dictionaries are used to store data values in key:value pairs.
	- A dictionary is a collection which is ordered, changeable and do not allow duplicates.

```
mydict = {"brand": "Ford", "model": "Mustang", "year": 1964}
print(mydict["brand"])

thisdict = {
  "brand": "Ford",
  "electric": False,
  "year": 1964,
  "colors": ["red", "white", "red"]
}
```

Loops:
- Python has two primary types of loops: the for loop and the while loop.
- A for loop is used for iterating over a sequence (that is either a list, a tuple, a dictionary, a set, or a string).
- With the for loop we can execute a set of statements, once for each item in a list, tuple, set etc.

```
# Demo for loop with string and list of fruits
fruits = ["apple", "banana", "cherry"]
for fruit in fruits:
    print(fruit)
```

- Looping Through a String

```
# Demo for loop with string
print("\nCharacters in the word 'banana':")
for char in "banana":
    print(char)
```

- With the break statement we can stop the loop before it has looped through all the items:

```
# Demo break statement
fruits = ["apple", "banana", "cherry"]
print("\nDemo break statement:")
for fruit in fruits:
    if fruit == "banana":
        break
    print(fruit)
```

- With the continue statement we can stop the current iteration of the loop, and continue with the next:

```
# Do not print banana:
fruits = ["apple", "banana", "cherry"]
print("\nDemo continue statement:")
for fruit in fruits:
    if fruit == "banana":
        continue
    print(fruit)
```

- To loop through a set of code a specified number of times, we can use the range() function.
- The range() function returns a sequence of numbers, starting from 0 by default, and increments by 1 (by default), and ends at a specified number.

```
# Demo range function
print("\nDemo range function:")
for i in range(6):
    print(i)
```

- range function with starting value

```
# Demo range with start and end
print("\nDemo range with start and end:")
for i in range(2, 6):
    print(i)
```

- range function with increment value
- The range() function defaults to increment the sequence by 1, however it is possible to specify the increment value by adding a third parameter: range(2, 30, 3):

```
# Demo range with step
print("\nDemo range with step:")
for i in range(3, 30, 3):
    print(i)
```

- The else keyword in a for loop specifies a block of code to be executed when the loop is finished:

```
# Demo else statement with for loop
fruits = ["apple", "banana", "cherry"]
print("\nDemo else statement with for loop:")
for fruit in fruits:    
    print(fruit)
else:
    print("No more fruits in the list.")
```

Nested Loops
- A nested loop is a loop inside a loop.
- The "inner loop" will be executed one time for each iteration of the "outer loop":

```
# Demo nested loop
print("\nDemo nested loop:")
adj = ["red", "big", "tasty"]
fruits = ["apple", "banana", "cherry"]
for x in adj:
    for y in fruits:
        print(x, y)
```

- Print multiplication table of 4 in format 4 x i = result  

```
print("\nMultiplication table of 4:")
for i in range(1, 13):
    print("4 x", i, "=", 4 * i)
```

The pass statement 
- for loops cannot be empty, but if you for some reason have a for loop with no content, put in the pass statement to avoid getting an error.

```
# Using pass in for loop
print("\nUse pass in for loop:")
for i in range(5):
    pass
print("Loop completed.")
```
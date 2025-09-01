# PyStoreJSONDB

A lightweight, file-based JSON database controller and manager written in Python. 

Ideal for small applications, embedded systems, scripts, or local tools where simplicity, human-readability, and portability are priorities.

## Features
- File-based storage using JSON format.
- Insert, update, delete, and query operations.
- Sort Rows by key values
- Sort Columns by Reference Row or Provided List
- Rename column names
- Dynamic Column-ing:
    - Dynamic schema (no predefined columns)
    - insertion of a row with a new column adds that column to all other rows with value None
    - deletion or editing of a row that results in the column being empty will delete the column
- Sorting of columns by index or by a provided list.
- Print database contents in a human-readable format.
- No external dependencies (uses only Python standard library)

## Advantages of PyStoreJSONDB
1. Lightweight and Dependency-Free
2. Human-Readable and Editable
3. File-Based and Portable
4. No Schema Enforcement
5. Easy Integration with Python
6. Dynamic Columns allow for flexible data structure

## Disclaimer
**DO NOT USE THIS FOR PRODUCTION USE**\
This is intended for small-scale projects and scripts.

Not designed for high-performance or production-grade applications.\
Please use a more robust database solution, such as PostgreSQL or MongoDB for these applications.

## Installation
```cmd
pip install git+https://github.com/CarrotBRRR/PyStoreJSON.git
```

## Usage
### Initialize Manager

```python
from PyStoreJSONLib import PyStoreManager

manager = PyStoreManager("path/to/databases")
```
This will create a directory if it does not exist.\
Each manager is a collection of JSON files that represent datatables.

### Create and Get Database
```python
db = manager.create_database("people")
db = manager.get_database("people")
```

### Insert Rows
```python
db.insert({"name": "Alice", "age": 30})
db.insert({"name": "Charlie", "age": 30})
db.insert({"name": "Eve", "age": 29, "city": "New York"})
db.insert(
    [
        {"name": "Bob", "age": 25, "city": "Los Angeles"},
        {"name": "Ivy", "age": 29, "city": None},
        {"name": "Hannah", "age": 30}
    ]
)
```

### Query Rows
```python
results = db.query({"age": 30})
print(results)
```
this will print out somthing like
```json
[
    {"name": "Alice", "age": 30},
    {"name": "Charlie", "age": 30}
]
```

### Update Rows
```python
db.update({"age": 30}, {"age": 31})
```
this will update all rows with age 30 to age 31

### Delete Rows
```python
db.delete({"age": 31})
```
this will delete all rows with age 31

### Rename Column
```python
db.rename_key("name", "nickname")
```
This will rename the column "name" to "nickname" in all rows.

### Sort Rows
```python
db.sort("age", reverse=True)
```
This will sort the rows by the "age" column in descending order.

### Sort Columns by Reference Row
```python
db.sort_columns(0)
```
This will sort the columns to match the order of the first row.

### Sort Columns by Provided List
```python
db.sort_columns(["age", "name", "city"])
```
This will sort the columns to the order specified by the provided list of indices.\
The list can contain column names or indices, and any columns not in the list, and will not affect the order of the columns.

### Print Database
```python
manager.print_database("people")
```
This will print out the entire contents of the "people" database in a human-readable format.

# Moving data

## Table of contents
1. [Introduction](README.md#introduction)
2. [Implementation](README.md#implementation)
3. [How to run](README.md#how-to-run)

## Introduction

We are provided with a zip file containing JSON documents listing orders. We have to put these JSON documents in a PostgreSQL database.

## Implementation

I used the Python library psycopg2 to interface with PostgreSQL.

Here is the logic of the `main()` function:
```
open zip file
for each JSON document
    convert document to Python objects (list of dictionaries)
    for each "order" element in the list of dictionaries
        collect list of "line_items"
        insert "order" in a PostgreSQL "orders" table
        for each item in the "line_items" list
            insert item in the "items" table
```

To prevent SQL injection with dynamically generated SQL executable strings, I use either the `sql.Identifier` for table names or direct string composition in the `cursor.execute()` function argument.

## How to run

Place input zip files in the `input` folder at the root of the repository, and execute `run.sh`.

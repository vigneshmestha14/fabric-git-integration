# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # **Combine Two Tables**
# 
# ## **Problem Statement**
# Given two tables:
# 
# ### **Table: Person**
# | Column Name | Type    |
# |-------------|--------|
# | `PersonId`  | int    |
# | `FirstName` | varchar |
# | `LastName`  | varchar |
# 
# - `PersonId` is the **primary key**.
# - This table contains **personal details**.
# 
# ### **Table: Address**
# | Column Name | Type    |
# |-------------|--------|
# | `AddressId` | int    |
# | `PersonId`  | int    |
# | `City`      | varchar |
# | `State`     | varchar |
# 
# - `AddressId` is the **primary key**.
# - `PersonId` is a **foreign key** referencing `Person.PersonId`.
# - This table contains **address details**.
# 
# ### **Task**
# Write a **query** to generate a report with:
# - `FirstName`
# - `LastName`
# - `City`
# - `State`
# 
# The result should include **all persons**, even if they **don't have an address**.
# 
# ---
# 
# ## **Example**
# 
# ### **Input:**
# 
# **Person Table**
# | PersonId | FirstName | LastName |
# |----------|----------|----------|
# | 1        | John     | Doe      |
# | 2        | Jane     | Smith    |
# | 3        | Alice    | Brown    |
# 
# **Address Table**
# | AddressId | PersonId | City    | State  |
# |-----------|---------|---------|--------|
# | 1         | 1       | New York| NY     |
# | 2         | 2       | Boston  | MA     |
# 
# ### **Output:**
# | FirstName | LastName | City    | State  |
# |-----------|---------|---------|--------|
# | John      | Doe     | New York| NY     |
# | Jane      | Smith   | Boston  | MA     |
# | Alice     | Brown   | NULL    | NULL   |
# 
# ---


# MARKDOWN ********************

# ## **Approach 1: PySpark DataFrame API**
# ### **Steps**
# 1. **Initialize Spark Session**  
#    - Create a Spark session.
# 2. **Load Data**  
#    - Read `Person.csv` and `Address.csv` into PySpark DataFrames.
# 3. **Perform LEFT JOIN on `PersonId`**  
#    - Use `join()` function with `"left"` join type.
# 4. **Select required columns**  
#    - Retrieve `FirstName`, `LastName`, `City`, and `State`.
# 5. **Display the Output**  
# 
# ### **Code**

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Step 1: Initialize Spark Session
spark = SparkSession.builder.appName("CombineTwoTables").getOrCreate()

# Step 2: Create Sample Data for Person Table
person_data = [
    (1, "John", "Doe"),
    (2, "Jane", "Smith"),
    (3, "Alice", "Brown")
]
person_columns = ["PersonId", "FirstName", "LastName"]

person_df = spark.createDataFrame(person_data, schema=person_columns)

# Step 3: Create Sample Data for Address Table
address_data = [
    (1, 1, "New York", "NY"),
    (2, 2, "Boston", "MA")
]
address_columns = ["AddressId", "PersonId", "City", "State"]

address_df = spark.createDataFrame(address_data, schema=address_columns)

# Step 4: Perform LEFT JOIN on PersonId
result_df = person_df.join(address_df, "PersonId", "left").select(
    col("FirstName"), col("LastName"), col("City"), col("State")
)

# Step 5: Display the Output
result_df.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# 
# ## **Approach 2: SQL Query in PySpark**
# ### **Steps**
# 1. **Initialize Spark Session**  
#    - Create a Spark session.
# 2. **Load Data and Create DataFrames**  
#    - Read `Person.csv` and `Address.csv` into PySpark DataFrames.
# 3. **Create Temporary SQL Views**  
#    - Register `Person` and `Address` as **temporary tables**.
# 4. **Write and Execute SQL Query**  
#    - Perform **LEFT JOIN** on `PersonId` to include all persons.
# 5. **Show Results**  
# 
# ### **Code**

# CELL ********************

# Step 1-3: Create Temporary Views
person_df.createOrReplaceTempView("Person")
address_df.createOrReplaceTempView("Address")

# Step 4: Run SQL Query
query = """
SELECT p.FirstName, p.LastName, a.City, a.State
FROM Person p
LEFT JOIN Address a
ON p.PersonId = a.PersonId
"""

# Step 5: Execute SQL Query
sql_result = spark.sql(query)

# Step 6: Show Output
sql_result.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# 
# ## **Summary**
# | Approach | Method | Steps |
# |----------|--------|-------|
# | **Approach 1** | PySpark DataFrame API | Uses `join()` with `"left"` join type |
# | **Approach 2** | SQL Query in PySpark | Uses `LEFT JOIN` in SQL |


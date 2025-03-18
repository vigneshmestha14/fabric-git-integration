# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # **Swap Salary**
# 
# ## **Problem Statement**
# Given a table `Salary`:
# 
# | Column Name | Type    |
# |-------------|--------|
# | `id`        | int    |
# | `name`      | varchar |
# | `sex`       | ENUM('m', 'f') |
# | `salary`    | int    |
# 
# - `id` is the **primary key**.
# - The `sex` column contains values `'m'` (male) or `'f'` (female).
# - The table contains **employee salary details**.
# 
# ### **Task**
# Write a **query** to swap all `'m'` and `'f'` values in the `sex` column.
# 
# ---
# 
# ## **Example**
# 
# ### **Input:**
# **Salary Table**
# | id | name  | sex | salary |
# |----|------|-----|--------|
# | 1  | Alex | 'm' | 5000   |
# | 2  | Eve  | 'f' | 7000   |
# | 3  | Sam  | 'm' | 8000   |
# | 4  | Mia  | 'f' | 7500   |
# 
# ### **Output:**
# | id | name  | sex | salary |
# |----|------|-----|--------|
# | 1  | Alex | 'f' | 5000   |
# | 2  | Eve  | 'm' | 7000   |
# | 3  | Sam  | 'f' | 8000   |
# | 4  | Mia  | 'm' | 7500   |
# 
# ---

# MARKDOWN ********************

# 
# ## **Approach 1: PySpark DataFrame API**
# ### **Steps**
# 1. **Initialize Spark Session**  
#    - Create a Spark session to work with PySpark.
# 2. **Load Data**  
#    - Read the dataset `Salary.csv` into a PySpark DataFrame.
# 3. **Use `when()` and `col()` to swap 'm' and 'f'**  
#    - Use `F.when()` to **replace** 'm' with 'f' and vice versa.
# 4. **Select and Display Output**  
#    - Show the updated DataFrame.
# 
# ### **Code**


# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Step 1: Initialize Spark Session
spark = SparkSession.builder.appName("SwapSalary").getOrCreate()

# Step 2: Create a Sample Dataset
data = [
    (1, "Alex", "m", 5000),
    (2, "Eve", "f", 7000),
    (3, "Sam", "m", 8000),
    (4, "Mia", "f", 7500)
]
columns = ["id", "name", "sex", "salary"]

# Step 3: Create DataFrame
salary_df = spark.createDataFrame(data, schema=columns)

# Step 4: Swap 'm' with 'f' and vice versa
swapped_df = salary_df.withColumn(
    "sex",
    when(col("sex") == "m", "f").when(col("sex") == "f", "m").otherwise(col("sex"))
)

# Step 5: Display the Output
swapped_df.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# ## **Approach 2: SQL Query in PySpark**
# ### **Steps**
# 1. **Initialize Spark Session**  
#    - Create a Spark session.
# 2. **Load Data and Create DataFrame**  
#    - Read `Salary.csv` into a PySpark DataFrame.
# 3. **Create a Temporary SQL View**  
#    - Register the DataFrame as a **temporary SQL table**.
# 4. **Write and Execute SQL Query**  
#    - Use `CASE` statement to **swap 'm' with 'f'** and vice versa.
# 5. **Show Results**  
#    - Execute the query and display the output.
# 
# ### **Code**

# CELL ********************

# Step 1-3: Create Temporary View
salary_df.createOrReplaceTempView("Salary")

# Step 4: Run SQL Query
query = """
SELECT id, name, 
       CASE 
           WHEN sex = 'm' THEN 'f' 
           WHEN sex = 'f' THEN 'm' 
           ELSE sex 
       END AS sex, 
       salary
FROM Salary
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

# ## **Summary**
# | Approach | Method | Steps |
# |----------|--------|-------|
# | **Approach 1** | PySpark DataFrame API | Uses `when()` and `col()` to swap values |
# | **Approach 2** | SQL Query in PySpark | Uses `CASE` statement in SQL |

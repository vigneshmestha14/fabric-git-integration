# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # **Big Countries**
# ## **Problem Statement**
# There is a table **World** with the following schema:
# 
# | name         | continent | area     | population | gdp       |
# |-------------|-----------|----------|------------|-----------|
# | Afghanistan | Asia      | 652230   | 25500100   | 20343000  |
# | Albania     | Europe    | 28748    | 2831741    | 12960000  |
# | Algeria     | Africa    | 2381741  | 37100000   | 188681000 |
# | Andorra     | Europe    | 468      | 78115      | 3712000   |
# | Angola      | Africa    | 1246700  | 20609294   | 100990000 |
# 
# A country is **big** if it has:
# - An **area** greater than **3,000,000** square km, or
# - A **population** greater than **25,000,000**.
# 
# ### **Expected Output**
# | name        | population | area     |
# |------------|------------|----------|
# | Afghanistan | 25500100  | 652230   |
# | Algeria     | 37100000  | 2381741  |
# 
# ---

# MARKDOWN ********************

# ## **Approach 1: PySpark DataFrame API**
# ### **Steps**
# 1. **Initialize Spark Session**  
#    Create a Spark session to process the data.
# 2. **Load Data**  
#    Load the `World` dataset as a PySpark DataFrame.
# 3. **Apply Filters**  
#    Use PySpark's `filter()` method to select countries where either:
#    - The area is greater than **3,000,000** square km.
#    - The population is greater than **25,000,000**.
# 4. **Select Required Columns**  
#    Extract only the `name`, `population`, and `area` columns.
# 5. **Show Results**  
#    Display the output.
# 
# ### **Code**

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Step 1: Initialize Spark Session
spark = SparkSession.builder.appName("BigCountries").getOrCreate()

# Step 2: Create Data (Simulating World Table)
data = [
    ("Afghanistan", "Asia", 652230, 25500100, 20343000),
    ("Albania", "Europe", 28748, 2831741, 12960000),
    ("Algeria", "Africa", 2381741, 37100000, 188681000),
    ("Andorra", "Europe", 468, 78115, 3712000),
    ("Angola", "Africa", 1246700, 20609294, 100990000)
]

columns = ["name", "continent", "area", "population", "gdp"]

# Step 3: Create DataFrame
df = spark.createDataFrame(data, columns)

# Step 4: Apply Filter Conditions
filtered_df = df.filter((col("area") > 3000000) | (col("population") > 25000000))

# Step 5: Select Required Columns
result_df = filtered_df.select("name", "population", "area")

# Step 6: Show Output
result_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# ## **Approach 2: SQL Query in PySpark**
# ### **Steps**
# 1. **Initialize Spark Session**  
#    Create a Spark session.
# 2. **Load Data and Create DataFrame**  
#    Load the dataset into a PySpark DataFrame.
# 3. **Create a Temporary SQL View**  
#    Register the DataFrame as a SQL table (`world`).
# 4. **Write and Execute SQL Query**  
#    - Fetch countries where `area > 3,000,000` OR `population > 25,000,000`.
#    - Select only the `name`, `population`, and `area` columns.
# 5. **Show Results**  
#    Execute and display the query output.
# 
# ### **Code**

# CELL ********************

# Step 1-3: Create Temporary View
df.createOrReplaceTempView("world")

# Step 4: Run SQL Query
query = """
SELECT name, population, area
FROM world
WHERE area > 3000000 OR population > 25000000
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
# ## **Summary**
# | Approach | Method | Steps |
# |----------|--------|-------|
# | **Approach 1** | PySpark DataFrame API | Uses `filter()` and `select()` on a DataFrame |
# | **Approach 2** | SQL Query in PySpark | Uses `createOrReplaceTempView()` and `spark.sql()` |


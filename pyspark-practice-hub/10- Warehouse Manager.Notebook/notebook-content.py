# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# 


# MARKDOWN ********************

# # **Warehouse Manager**
# 
# ## **Problem Statement**
# Given the following tables:
# 
# ### **Table: Warehouse**
# | Column Name  | Type    |
# |-------------|---------|
# | `name`      | varchar |
# | `product_id`| int     |
# | `units`     | int     |
# 
# - **(name, product_id) is the primary key.**
# - Contains information about **which warehouse stores how many units** of each product.
# 
# ### **Table: Products**
# | Column Name   | Type    |
# |--------------|---------|
# | `product_id` | int     |
# | `product_name` | varchar |
# | `Width`      | int     |
# | `Length`     | int     |
# | `Height`     | int     |
# 
# - **product_id is the primary key.**
# - Contains information about **product dimensions (width, length, height) in feet**.
# 
# ### **Objective**
# Write an SQL query to **calculate how much cubic feet of volume the inventory occupies in each warehouse**.
# 
# ---
# 
# ## **Example**
# 
# ### **Input:**
# 
# #### **Warehouse Table**
# | name     | product_id | units |
# |----------|------------|-------|
# | LCHouse1 | 1          | 1     |
# | LCHouse1 | 2          | 10    |
# | LCHouse1 | 3          | 5     |
# | LCHouse2 | 1          | 2     |
# | LCHouse2 | 2          | 2     |
# | LCHouse3 | 4          | 1     |
# 
# #### **Products Table**
# | product_id | product_name | Width | Length | Height |
# |------------|-------------|------|--------|--------|
# | 1          | LC-TV       | 5    | 50     | 40     |
# | 2          | LC-KeyChain | 5    | 5      | 5      |
# | 3          | LC-Phone    | 2    | 10     | 10     |
# | 4          | LC-T-Shirt  | 4    | 10     | 20     |
# 
# ---


# MARKDOWN ********************

# ---
# 
# ## **Approach 1: PySpark DataFrame API**
# ### **Steps**
# 1. **Initialize Spark Session**
# 2. **Create DataFrames for Warehouse and Products**
# 3. **Calculate Product Volume (Width × Length × Height)**
# 4. **Join Warehouse with Products on `product_id`**
# 5. **Compute Total Volume for Each Warehouse**
# 6. **Display the Output**
# ### **Code**

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, sum

# Step 1: Initialize Spark Session
spark = SparkSession.builder.appName("WarehouseVolume").getOrCreate()

# Step 2: Create DataFrames
warehouse_data = [("LCHouse1", 1, 1), ("LCHouse1", 2, 10), ("LCHouse1", 3, 5),
                  ("LCHouse2", 1, 2), ("LCHouse2", 2, 2), ("LCHouse3", 4, 1)]
warehouse_columns = ["name", "product_id", "units"]

product_data = [(1, "LC-TV", 5, 50, 40), (2, "LC-KeyChain", 5, 5, 5), 
                (3, "LC-Phone", 2, 10, 10), (4, "LC-T-Shirt", 4, 10, 20)]
product_columns = ["product_id", "product_name", "Width", "Length", "Height"]

warehouse_df = spark.createDataFrame(warehouse_data, warehouse_columns)
product_df = spark.createDataFrame(product_data, product_columns)

# Step 3: Compute Product Volume
product_df = product_df.withColumn("volume_per_unit", col("Width") * col("Length") * col("Height"))

# Step 4: Join Warehouse and Products on product_id
joined_df = warehouse_df.join(product_df, "product_id", "inner")

# Step 5: Compute Total Volume for Each Warehouse
result_df = joined_df.withColumn("total_volume", col("units") * col("volume_per_unit")) \
                     .groupBy("name") \
                     .agg(sum("total_volume").alias("volume"))

# Step 6: Display Output
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
# 1. **Create Spark Session**
# 2. **Create DataFrames for Warehouse and Products**
# 3. **Register Them as SQL Views**
# 4. **Write and Execute SQL Query**
# 5. **Display the Output**

# CELL ********************

from pyspark.sql import SparkSession

# Step 1: Initialize Spark Session
spark = SparkSession.builder.appName("WarehouseSQL").getOrCreate()

# Step 2: Create DataFrames
warehouse_df.createOrReplaceTempView("Warehouse")
product_df.createOrReplaceTempView("Products")

# Step 3: Run SQL Query
sql_query = """
SELECT w.name AS warehouse_name, 
       SUM(w.units * (p.Width * p.Length * p.Height)) AS volume
FROM Warehouse w
JOIN Products p ON w.product_id = p.product_id
GROUP BY w.name
"""

result_sql = spark.sql(sql_query)

# Step 4: Display Output
result_sql.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# ---
# 
# ## **Summary**
# | Approach  | Method                      | Steps  |
# |-----------|-----------------------------|--------|
# | **Approach 1** | PySpark DataFrame API    | Uses `withColumn()`, `join()`, and `groupBy().agg()` |
# | **Approach 2** | SQL Query in PySpark     | Uses SQL `JOIN` and `SUM()` |
# 
# Both approaches return the **same correct result**.

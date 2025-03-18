# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # **Rank Scores**
# 
# ## **Problem Statement**
# Given a `Scores` table:
# 
# ### **Table: Scores**
# | Column Name | Type  |
# |-------------|------|
# | `Id`        | int  |
# | `Score`     | float |
# 
# - The table contains **student scores**.
# - The ranking should:
#   - **Be ordered by highest score.**
#   - **Handle ties correctly (same rank for duplicate values).**
#   - **Ensure no gaps in ranking numbers (next rank should be consecutive).**
# 
# ---
# 
# ## **Example**
# 
# ### **Input:**
# | Id | Score |
# |----|-------|
# | 1  | 3.50  |
# | 2  | 3.65  |
# | 3  | 4.00  |
# | 4  | 3.85  |
# | 5  | 4.00  |
# | 6  | 3.65  |
# 
# ### **Expected Output:**
# | Score | Rank |
# |-------|------|
# | 4.00  | 1    |
# | 4.00  | 1    |
# | 3.85  | 2    |
# | 3.65  | 3    |
# | 3.65  | 3    |
# | 3.50  | 4    |
# 
# ---

# MARKDOWN ********************

# ## **Approach 1: PySpark DataFrame API**
# ### **Steps**
# 1. **Initialize Spark Session**  
#    - Create a Spark session.
# 2. **Load Data**  
#    - Read `Scores.csv` into a PySpark DataFrame.
# 3. **Apply Ranking**  
#    - Use `dense_rank()` to **assign ranks without gaps**.
#    - Use `desc("Score")` to rank **higher scores first**.
# 4. **Display the Output**  
# 
# ### **Code**

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank

# Step 1: Initialize Spark Session
spark = SparkSession.builder.appName("RankScores").getOrCreate()

# Step 2: Load Data into DataFrame
data = [(1, 3.50), (2, 3.65), (3, 4.00), (4, 3.85), (5, 4.00), (6, 3.65)]
columns = ["Id", "Score"]
scores_df = spark.createDataFrame(data, columns)

# Step 3: Define Window Specification
window_spec = Window.orderBy(col("Score").desc())

# Step 4: Compute Dense Rank
ranked_df = scores_df.withColumn("Rank", dense_rank().over(window_spec))

# Step 5: Display the Output
ranked_df.select("Score", "Rank").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Approach 2: SQL Query in PySpark**
# ### **Steps**
# 1. **Create Temporary SQL Views**  
#    - Register `Scores` as a **temporary table**.
# 2. **Write and Execute SQL Query**  
#    - Use `DENSE_RANK()` **to avoid gaps**.
#    - Use `ORDER BY Score DESC` **to rank higher scores first**.
# 3. **Show Results**  
# 
# ### **Code**

# CELL ********************

# Step 1: Create Temporary SQL View
scores_df.createOrReplaceTempView("Scores")

# Step 2: Run SQL Query
query = """
SELECT Score, DENSE_RANK() OVER (ORDER BY Score DESC) AS Rank
FROM Scores
"""

# Step 3: Execute SQL Query
sql_result = spark.sql(query)

# Step 4: Show Output
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
# | **Approach 1** | PySpark DataFrame API | Uses `dense_rank()` function with `Window` |
# | **Approach 2** | SQL Query in PySpark | Uses `DENSE_RANK()` in SQL |

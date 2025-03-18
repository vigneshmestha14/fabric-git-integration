# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # **Director's Actor**
# 
# ## **Problem Statement**
# Given a table `ActorDirector`:
# 
# | Column Name  | Type  |
# |-------------|-------|
# | `actor_id`  | int   |
# | `director_id` | int   |
# | `timestamp`  | int   |
# 
# - `timestamp` is the **primary key** of this table.
# - Write a **SQL query** to output pairs **(actor_id, director_id)** where the actor has worked with the director **at least 3 times**.
# 
# ### **Example**
# 
# #### **Input:**
# **ActorDirector Table**
# | actor_id | director_id | timestamp |
# |----------|------------|-----------|
# | 1        | 1          | 0         |
# | 1        | 1          | 1         |
# | 1        | 1          | 2         |
# | 1        | 2          | 3         |
# | 1        | 2          | 4         |
# | 2        | 1          | 5         |
# | 2        | 1          | 6         |
# 
# #### **Output:**
# | actor_id | director_id |
# |----------|------------|
# | 1        | 1          |
# 
# - The only valid pair is **(1,1)** because they co-worked **exactly 3 times**.

# MARKDOWN ********************

# ---
# 
# ## **Approach 1: PySpark DataFrame API**
# ### **Steps**
# 1. **Initialize Spark Session**  
#    - Create a Spark session to work with PySpark.
# 2. **Load Data**  
#    - Read the dataset `ActorDirector.csv` into a PySpark DataFrame.
# 3. **Group by Actor-Director Pair**  
#    - Use `groupBy("actor_id", "director_id")` and count the occurrences.
# 4. **Filter for at least 3 occurrences**  
#    - Apply `.filter(count >= 3)`.
# 5. **Select and Display Output**  
#    - Show the final DataFrame.
# 
# ### **Code**

# CELL ********************


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# Step 1: Initialize Spark Session
spark = SparkSession.builder.appName("DirectorActor").getOrCreate()

# Step 2: Create the DataFrame
data = [
    (1, 1, 0),
    (1, 1, 1),
    (1, 1, 2),
    (1, 2, 3),
    (1, 2, 4),
    (2, 1, 5),
    (2, 1, 6)
]

# Create the DataFrame
actor_director_df = spark.createDataFrame(data, ["actor_id","director_id","timestamp"])

# Step 3: Group by Actor and Director & Count Occurrences
grouped_df = actor_director_df.groupBy("actor_id", "director_id").agg(count("*").alias("count"))

# Step 4: Filter for pairs where count >= 3
filtered_df = grouped_df.filter(col("count") >= 3).select("actor_id", "director_id")

# Step 5: Display the Output
filtered_df.show()


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
# 2. **Load Data and Create DataFrame**  
#    - Read `ActorDirector.csv` into a PySpark DataFrame.
# 3. **Create a Temporary SQL View**  
#    - Register the DataFrame as a **temporary SQL table**.
# 4. **Write and Execute SQL Query**  
#    - **Group by** `actor_id`, `director_id`.
#    - Use `COUNT(*) >= 3` to filter valid pairs.
# 5. **Show Results**  
#    - Execute the query and display the output.
# 
# ### **Code**

# CELL ********************

# Step 1-3: Create Temporary View
actor_director_df.createOrReplaceTempView("ActorDirector")

# Step 4: Run SQL Query
query = """
SELECT actor_id, director_id
FROM ActorDirector
GROUP BY actor_id, director_id
HAVING COUNT(*) >= 3
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
# | **Approach 1** | PySpark DataFrame API | Uses `groupBy()`, `agg(count)`, and `filter()` |
# | **Approach 2** | SQL Query in PySpark | Uses `GROUP BY`, `HAVING COUNT(*) >= 3` |


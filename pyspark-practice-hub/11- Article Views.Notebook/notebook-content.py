# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # **Article Views**
# 
# ## **Problem Statement**
# Given the following table:
# 
# ### **Table: Views**
# | Column Name | Type  |
# |------------|-------|
# | `article_id` | int |
# | `author_id` | int |
# | `viewer_id` | int |
# | `view_date` | date |
# 
# - The table records **which viewer** viewed **which article** on a given date.
# - The table **may have duplicate rows**.
# - **A viewer can also be the author of an article**, but this should not be excluded.
# - **A person can view multiple articles on the same day.**
# 
# ### **Objective**
# Write a SQL query to **find all the people (viewer_id) who viewed more than one article on the same date, sorted in ascending order**.
# 
# ---
# 
# ## **Example**
# 
# ### **Input:**
# 
# #### **Views Table**
# | article_id | author_id | viewer_id | view_date  |
# |------------|-----------|-----------|------------|
# | 1          | 3         | 5         | 2019-08-01 |
# | 3          | 4         | 5         | 2019-08-01 |
# | 1          | 3         | 6         | 2019-08-02 |
# | 2          | 7         | 7         | 2019-08-01 |
# | 2          | 7         | 6         | 2019-08-02 |
# | 4          | 7         | 1         | 2019-07-22 |
# | 3          | 4         | 4         | 2019-07-21 |
# | 3          | 4         | 4         | 2019-07-21 |
# 
# ---
# 
# ### **Expected Output:**
# | id |
# |----|
# | 5  |
# | 6  |
# 
# **Explanation:**
# - `viewer_id = 5` viewed **two different articles** (`article_id = 1, 3`) on `2019-08-01`.
# - `viewer_id = 6` viewed **two different articles** (`article_id = 1, 2`) on `2019-08-02`.
# - `viewer_id = 4` **viewed the same article twice** (`article_id = 3`), but **on the same date**, so they are **not included**.
# 
# ---


# MARKDOWN ********************

# ## **Approach 1: PySpark DataFrame API**
# ### **Steps**
# 1. **Initialize Spark Session**
# 2. **Create a DataFrame for `Views` Table**
# 3. **Group by `viewer_id` and `view_date`, count distinct articles**
# 4. **Filter viewers who viewed more than 1 distinct article on the same day**
# 5. **Select `viewer_id` and rename as `id`**
# 6. **Sort results in ascending order**
# 7. **Display Output**
# 
# ### **Code**

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct

# Step 1: Initialize Spark Session
spark = SparkSession.builder.appName("ArticleViews").getOrCreate()

# Step 2: Create DataFrame for Views Table
views_data = [
    (1, 3, 5, "2019-08-01"),
    (3, 4, 5, "2019-08-01"),
    (1, 3, 6, "2019-08-02"),
    (2, 7, 7, "2019-08-01"),
    (2, 7, 6, "2019-08-02"),
    (4, 7, 1, "2019-07-22"),
    (3, 4, 4, "2019-07-21"),
    (3, 4, 4, "2019-07-21"),
]
views_columns = ["article_id", "author_id", "viewer_id", "view_date"]

views_df = spark.createDataFrame(views_data, views_columns)

# Step 3: Group by viewer_id and view_date, count distinct articles
grouped_df = views_df.groupBy("viewer_id", "view_date").agg(countDistinct("article_id").alias("article_count"))

# Step 4: Filter viewers who viewed more than 1 distinct article on the same day
filtered_df = grouped_df.filter(col("article_count") > 1)

# Step 5: Select viewer_id and rename as id
result_df = filtered_df.select(col("viewer_id").alias("id")).distinct()

# Step 6: Sort results in ascending order
result_df = result_df.orderBy("id")

# Step 7: Display Output
result_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# ---
# 
# ## **Approach 2: SQL Query in PySpark**
# ### **Steps**
# 1. **Create Spark Session**
# 2. **Create DataFrame for Views Table**
# 3. **Register it as a SQL View**
# 4. **Write and Execute SQL Query**
# 5. **Display the Output**
# 
# ### **Code**

# CELL ********************

# Step 1: Initialize Spark Session
spark = SparkSession.builder.appName("ArticleViewsSQL").getOrCreate()

# Step 2: Register DataFrame as a SQL View
views_df.createOrReplaceTempView("Views")

# Step 3: Run SQL Query
sql_query = """
SELECT viewer_id AS id
FROM Views
GROUP BY viewer_id, view_date
HAVING COUNT(DISTINCT article_id) > 1
ORDER BY viewer_id;
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

# ---
# 
# ## **Summary**
# | Approach  | Method                      | Steps  |
# |-----------|-----------------------------|--------|
# | **Approach 1** | PySpark DataFrame API    | Uses `groupBy().agg()`, `countDistinct()`, and `filter()` |
# | **Approach 2** | SQL Query in PySpark     | Uses SQL `GROUP BY`, `HAVING COUNT(DISTINCT)`, and `ORDER BY` |


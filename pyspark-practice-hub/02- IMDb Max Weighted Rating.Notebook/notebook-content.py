# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1f4688c4-9112-42ba-8c5a-2c7045d78522",
# META       "default_lakehouse_name": "lh_sql_practice_hub",
# META       "default_lakehouse_workspace_id": "c6598bd0-bf59-4b02-a761-73171d8c2571"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # **IMDb Max Weighted Rating**
# 
# ## **Problem Statement**
# Print the **genre** and the **maximum weighted rating** among all the movies of that genre released in **2014** per genre.
# 
# ### **Notes**
# 1. **Do not print** any row where **either genre or the weighted rating** is empty/null.
# 2. **Weighted rating** is calculated as:  
#    `weighted_rating = (rating + metacritic / 10.0) / 2.0`
# 3. The **output columns** should be named as:
#    - `genre`
#    - `weighted_rating`
# 4. The **genres should be printed in alphabetical order**.
# 
# ---

# MARKDOWN ********************

# 
# ## **Approach 1: PySpark DataFrame API**
# ### **Steps**
# 1. **Initialize Spark Session**  
#    - Create a Spark session to work with PySpark.
# 2. **Load Data**  
#    - Read datasets: `earning.csv`, `IMDB.csv`, and `genre.csv` into PySpark DataFrames.
# 3. **Join Datasets**  
#    - Use a **left join** to merge `earning` with `IMDB` on `movie_id`.
#    - Use another **left join** to merge `genre` on `movie_id`.
# 4. **Filter Data**  
#    - Select movies **released in 2014** (`title` contains "2014").
#    - Ensure **genre is not null**.
# 5. **Calculate Weighted Rating**  
#    - Compute `weighted_rating = (rating + metacritic / 10.0) / 2.0`.
# 6. **Group and Aggregate**  
#    - **Group by `genre`** and compute **maximum `weighted_rating`** per genre.
# 7. **Filter and Sort**  
#    - Exclude rows where `weighted_rating` ≤ 0.
#    - Sort genres **alphabetically**.
# 8. **Display the Output**  
#    - Show the final DataFrame.
# 
# ### **Code**

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max

# Step 1: Initialize Spark Session
spark = SparkSession.builder.appName("IMDbMaxWeightedRating").getOrCreate()

# Step 2: Load Datasets
earning_df = spark.read.csv("Files/csv/earning.csv", header=True, inferSchema=True)
imdb_df = spark.read.csv("Files/csv/IMDB.csv", header=True, inferSchema=True)
genre_df = spark.read.csv("Files/csv/genre.csv", header=True, inferSchema=True)

# Step 3: Join Datasets
joined_df = (
    earning_df
    .join(imdb_df, "movie_id", "left")
    .join(genre_df, "movie_id", "left")
)

# Step 4: Filter Data (Only Movies Released in 2014 & Genre Not Null)
filtered_df = joined_df.filter(
    (col("title").like("%2014%")) & 
    (col("genre").isNotNull())
)

# Step 5: Calculate Weighted Rating
weighted_df = filtered_df.withColumn(
    "weighted_rating", (col("rating") + (col("metacritic") / 10.0)) / 2.0
)

# Step 6: Group by Genre and Get Maximum Weighted Rating
result_df = (
    weighted_df.groupBy("genre")
    .agg(max("weighted_rating").alias("weighted_rating"))
)

# Step 7: Filter and Sort the Results
final_df = result_df.filter(col("weighted_rating") > 0).orderBy("genre")

# Step 8: Display Output
final_df.show()

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
#    - Read the datasets: `earning.csv`, `IMDB.csv`, and `genre.csv`.
# 3. **Create Temporary SQL Views**  
#    - Register each DataFrame as a **temporary SQL table**.
# 4. **Write and Execute SQL Query**  
#    - **Join tables** using `LEFT JOIN`.
#    - **Filter movies released in 2014** (`title LIKE '%2014%'`).
#    - **Exclude null genres**.
#    - **Calculate weighted rating** as  
#      `weighted_rating = (rating + metacritic / 10.0) / 2.0`
#    - **Group by genre** and compute **max weighted rating**.
#    - **Exclude rows where weighted rating ≤ 0**.
#    - **Sort by genre alphabetically**.
# 5. **Show Results**  
#    - Execute the query and display the output.
# 
# ### **Code**


# CELL ********************

# Step 1-3: Create Temporary Views
earning_df.createOrReplaceTempView("earning")
imdb_df.createOrReplaceTempView("IMDB")
genre_df.createOrReplaceTempView("genre")

# Step 4: Run SQL Query
query = """
SELECT genre, MAX((rating + metacritic / 10.0) / 2.0) AS weighted_rating
FROM earning 
LEFT JOIN IMDB ON earning.movie_id = IMDB.movie_id AND title LIKE '%2014%'
LEFT JOIN genre ON earning.movie_id = genre.movie_id
WHERE genre IS NOT NULL
GROUP BY genre
HAVING MAX((rating + metacritic / 10.0) / 2.0) > 0
ORDER BY genre
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
# | **Approach 1** | PySpark DataFrame API | Uses `filter()`, `withColumn()`, `groupBy()`, and `orderBy()` |
# | **Approach 2** | SQL Query in PySpark | Uses `LEFT JOIN`, `GROUP BY`, `HAVING`, and `ORDER BY` |


# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # IMDb Metacritic Rating Analysis

# MARKDOWN ********************

# 
# ## Problem Statement
# 
# Print the title and ratings of the movies released in 2012 whose Metacritic rating is more than 60 and Domestic collections exceed 10 Crores.


# MARKDOWN ********************

# ## Approach 1: Using PySpark DataFrame API

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("IMDb Metacritic Analysis").getOrCreate()

# Load datasets
earning_df = spark.read.csv("Files/csv/earning.csv", header=True, inferSchema=True)
imdb_df = spark.read.csv("Files/csv/IMDB.csv", header=True, inferSchema=True)

# Join datasets
joined_df = earning_df.join(imdb_df, earning_df.Movie_id == imdb_df.Movie_id, "left")

# Filter conditions
filtered_movies = joined_df.filter(
    (col("domestic") > 100000000) & 
    (col("metacritic") > 60) & 
    (col("title").like("%2012%"))
)

# Select required columns
result = filtered_movies.select("title", "rating")

# Display results
result.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Approach 2: Using SQL Query in PySpark

# CELL ********************

# Create Temp Views
earning_df.createOrReplaceTempView("earning")
imdb_df.createOrReplaceTempView("IMDB")

# Execute SQL query in PySpark
query = """
SELECT IMDB.title, IMDB.rating 
FROM earning 
LEFT JOIN IMDB ON earning.movie_id = IMDB.movie_id 
WHERE earning.domestic > 100000000 
AND IMDB.metacritic > 60 
AND IMDB.title LIKE '%2012%'
"""

result = spark.sql(query)

# Show result
result.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

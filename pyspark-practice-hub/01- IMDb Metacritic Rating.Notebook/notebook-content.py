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
# META       "default_lakehouse_workspace_id": "c6598bd0-bf59-4b02-a761-73171d8c2571",
# META       "known_lakehouses": [
# META         {
# META           "id": "1f4688c4-9112-42ba-8c5a-2c7045d78522"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # **IMDb Metacritic Rating**
# 
# ## **Problem Statement**
# 
# Print the title and ratings of the movies released in 2012 whose Metacritic rating is more than 60 and Domestic collections exceed 10 Crores.

# MARKDOWN ********************

# ## **Approach 1: Using PySpark DataFrame API**
# 
# ## **Steps:**
# 1. **Initialize Spark Session**  
#    - Create a Spark session using `SparkSession.builder.appName()`.  
# 
# 2. **Load Datasets**  
#    - Read `earning.csv` and `IMDB.csv` using `spark.read.csv()`.  
#    - Ensure headers are recognized and infer schema automatically.  
# 
# 3. **Join the Datasets**  
#    - Perform a **left join** on `Movie_id` to combine earnings and IMDb data.  
# 
# 4. **Apply Filter Conditions**  
#    - Filter movies where:  
#      - Domestic earnings exceed **100 Crores (100,000,000)**.  
#      - Metacritic rating is greater than **60**.  
#      - The movie title contains **"2012"**.  
# 
# 5. **Select Required Columns**  
#    - Extract only **"title"** and **"rating"** columns from the filtered DataFrame.  
# 
# 6. **Display Results**  
#    - Show the final result using `display(result)`.  


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

# display results
display(result)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Approach 2: PySpark DataFrame API with Temp View and SQL Query**
# 
# ### **Steps:**
# 1. Create a PySpark DataFrame using the student records.
# 2. Register the DataFrame as a Temporary View using `createOrReplaceTempView()`.
# 3. Run SQL queries on the Temp View to fetch and display records.
# 4. Show the final result using `spark.sql()` to execute the query.

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
display(result)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # Split Function In DataFrame

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, size, array_contains, col

# Create a Spark session
spark = SparkSession.builder.appName("PySparkSplitFunctions").getOrCreate()

# Sample employee data
data = [
    (1, "Alice", "HR", "Communication Management"),
    (2, "Bob", "IT", "Programming Networking"),
    (3, "Charlie", "Finance", "Accounting Analysis"),
    (4, "David", "HR", "Recruiting Communication"),
    (5, "Eve", "IT", "Cloud DevOps")
]

# Define the schema
columns = ["EmployeeID", "Name", "Department", "Skills"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Display the original DataFrame
df.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. Split the 'Skills' Column

# CELL ********************

df_split = df.withColumn("Skills_Array", split(col("Skills"), ' '))
df_split.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Select the First Skill from 'Skills_Array'

# CELL ********************

df_first_skill = df_split.select(col("Name"), col("Skills_Array")[0].alias("First_Skill"))
df_first_skill.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Calculate the Size of 'Skills_Array'

# CELL ********************

df_skill_count = df_split.select(col("Name"), size(col("Skills_Array")).alias("Skills_Count"))
df_skill_count.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Check if 'Skills_Array' Contains a Specific Skill (e.g., 'Cloud')

# CELL ********************

df_cloud_check = df_split.select(col("Name"), array_contains(col("Skills_Array"), "Cloud").alias("Has_Cloud_Skill"))
df_cloud_check.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Explode 'Skills_Array' into Individual Rows

# CELL ********************

df_exploded = df_split.select(col("EmployeeID"), col("Name"), explode(col("Skills_Array")).alias("Individual_Skill"))
df_exploded.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Summary of Key Functions
# 
# - **split()**: This splits a column's string value into an array based on a specified delimiter (in this case, a space).
# - **explode()**: Converts an array column into multiple rows, one for each element in the array.
# - **size()**: Returns the number of elements in an array.
# - **array_contains()**: Checks if a specific value exists in the array.
# - **selectExpr()**: Allows you to use SQL expressions (like `array[0]`) to select array elements.

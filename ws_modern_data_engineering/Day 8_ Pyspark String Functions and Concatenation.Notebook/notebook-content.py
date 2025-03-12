# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # PySpark String Functions and Concatenation
# 
# This notebook demonstrates various string functions in PySpark.

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, concat_ws, initcap, lower, upper

# Create a Spark session
spark = SparkSession.builder.appName("SortingAndStringFunctions").getOrCreate()

# Sample data
data = [
    ("USA", "North America", 100, 50.5),
    ("India", "Asia", 300, 20.0),
    ("Germany", "Europe", 200, 30.5),
    ("Australia", "Oceania", 150, 60.0),
    ("Japan", "Asia", 120, 45.0),
    ("Brazil", "South America", 180, 25.0)
]

# Define the schema
columns = ["Country", "Region", "UnitsSold", "UnitPrice"]

# Create DataFrame
df = spark.createDataFrame(data, columns)
df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Convert the First Letter of Each Word to Uppercase

# CELL ********************

df_initcap = df.select(col("Country"), initcap(col("Country")).alias("Country_InitCap"))
df_initcap.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Convert All Text to Lowercase

# CELL ********************

df_lower = df.select(col("Country"), lower(col("Country")).alias("Country_Lower"))
df_lower.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Convert All Text to Uppercase

# CELL ********************

df_upper = df.select(col("Country"), upper(col("Country")).alias("Country_Upper"))
df_upper.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Concatenate Two Columns Without Separator

# CELL ********************

df_concat = df.select(col("Region"), col("Country"), concat(col("Region"), col("Country")).alias("Concatenated"))
df_concat.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Concatenate Two Columns With Separator

# CELL ********************

df_concat_sep = df.select(col("Region"), col("Country"), concat_ws(" | ", col("Region"), col("Country")).alias("Concatenated_With_Separator"))
df_concat_sep.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Create a New Concatenated Column

# CELL ********************

df_with_concat_column = df.withColumn("Region_Country", concat_ws(" ", col("Region"), col("Country")))
df_with_concat_column.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

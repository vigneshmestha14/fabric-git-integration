# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Sorting Functions

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

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

# Display the original DataFrame
df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 1. Sorting by a Single Column (Ascending Order)
# This example sorts the DataFrame by "UnitsSold" in ascending order.

# CELL ********************

# Sort by UnitsSold in Ascending Order
df_sorted_units = df.orderBy("UnitsSold")
df_sorted_units.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# # 2. Sorting by a Single Column (Descending Order)
# Sorting the DataFrame by "UnitPrice" in descending order.

# CELL ********************

# Sort by UnitPrice in Descending Order
df_sorted_price = df.orderBy(col("UnitPrice").desc())
df_sorted_price.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 3. Sorting by Multiple Columns
# Sorting first by "Region" (Ascending) and then by "UnitsSold" (Descending).


# CELL ********************

# Sort by Region (Ascending) and UnitsSold (Descending)
df_sorted_multi = df.orderBy(col("Region").asc(), col("UnitsSold").desc())
df_sorted_multi.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 4. Using `sort()` Function
# The `sort()` function provides the same functionality as `orderBy()`.

# CELL ********************

# Using sort() instead of orderBy()
df_sorted_sort = df.sort("UnitPrice")
df_sorted_sort.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 5. Sorting Using SQL Query
# Using SQL query to sort by "UnitsSold" in descending order.

# CELL ********************

df.createOrReplaceTempView("sales")

# Using SQL to sort
df_sorted_sql = spark.sql("SELECT * FROM sales ORDER BY UnitsSold DESC")
df_sorted_sql.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

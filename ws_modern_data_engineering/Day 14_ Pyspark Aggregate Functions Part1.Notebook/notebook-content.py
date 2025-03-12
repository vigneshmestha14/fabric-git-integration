# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # Aggregate Function in Pyspark – Part 1
# 


# MARKDOWN ********************

# ## Sample Data Frame

# CELL ********************

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import sum, avg, count, max, min, countDistinct

# Create Spark Session
spark = SparkSession.builder.appName("AggregateFunctions").getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create sample data
data = [
    Row(id=1, value=10),
    Row(id=2, value=20),
    Row(id=3, value=30),
    Row(id=4, value=None),
    Row(id=5, value=40),
    Row(id=6, value=20)
]

# Create DataFrame
df = spark.createDataFrame(data)

# Show the DataFrame
df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 1. **Summation (`sum`)**: Sums up the values in a specified column.

# CELL ********************

# Applying aggregate functions
total_sum = df.select(sum("value").alias("Total_sum")).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 2. **Average (`avg`)**: Computes the average of the values in a specified column.

# CELL ********************

average = df.select(avg("value").alias("Average")).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 3. **Count (`count`)**: Counts the number of non-null values in a specified column.

# CELL ********************

count = df.select(count("value").alias("Total_Count")).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 4. **Maximum (`max`) and Minimum (`min`)**: Finds the maximum and minimum values in a specified column.

# CELL ********************

max_min_value = df.select(max("value").alias("Max_value"),min("value").alias("Min_value")).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 5. **Distinct Values Count (`countDistinct`)**: Counts the number of distinct values in a specified column.

# CELL ********************

distinct_values = df.select(countDistinct("value").alias("DistinctCount")).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Notes:
# - **Handling Nulls**: The `count` function will count only non-null values, while `sum`, `avg`, `max`, and `min` will ignore null values in their calculations.
# - **Performance Considerations**: Aggregate functions can be resource-intensive, especially on large datasets. Using the appropriate partitioning can improve performance.
# 
# ### Use Cases:
# - **Summation**: Useful for calculating total sales, total revenue, etc.
# - **Average**: Helpful for finding average metrics like average sales per day.
# - **Count**: Useful for counting occurrences, such as the number of transactions.
# - **Max/Min**: Helps to determine the highest and lowest values, such as maximum sales on a specific day.
# - **Distinct Count**: Useful for finding unique items, like unique customers or products.

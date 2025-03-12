# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# ## Part 1: Date Functions in PySpark

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, current_timestamp, date_add, date_sub, datediff, months_between

# Create Spark session
spark = SparkSession.builder.appName("PySparkDateFunctions").getOrCreate()

# Sample data
data = [(1, "2024-01-01"), (2, "2023-06-15"), (3, "2022-12-31")]
columns = ["ID", "Date"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Convert column to date type
df = df.withColumn("Date", col("Date").cast("date"))

# Show DataFrame
df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Applying Common Date Functions
# 
# - **`current_date()`**: Returns the current system date.
# - **`current_timestamp()`**: Returns the current system timestamp.


# CELL ********************

df.select(current_date().alias("Current_Date"), current_timestamp().alias("Current_Timestamp")).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Date Arithmetic Functions
# - **`date_add(start_date, days)`**: Adds a specified number of days to the date.
# - **`date_sub(start_date, days)`**: Subtracts a specified number of days from the date.
# - **`datediff(end_date, start_date)`**: Returns the difference in days between two dates.
# - **`months_between(end_date, start_date)`**: Returns the difference in months between two dates.


# CELL ********************

df_date = df.select(
    col("Date"),
    date_add(col("Date"), 10).alias("Date_Add_10_Days"),
    date_sub(col("Date"), 5).alias("Date_Sub_5_Days")
)
df_date.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

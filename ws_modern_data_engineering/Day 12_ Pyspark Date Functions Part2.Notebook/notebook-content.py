# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# ## Part 2: Advanced Date Functions in PySpark

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

# CELL ********************

from pyspark.sql.functions import year, month, dayofmonth, dayofweek, weekofyear, date_format

# Extracting date parts
df_extracted = df.select(
    col("Date"),
    year(col("Date")).alias("Year"),
    month(col("Date")).alias("Month"),
    dayofmonth(col("Date")).alias("DayOfMonth"),
    dayofweek(col("Date")).alias("DayOfWeek"),
    weekofyear(col("Date")).alias("WeekOfYear")
)
df_extracted.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Formatting Dates
# - **`date_format(date, format)`**: Converts date into a formatted string.
# 
# Example formats:
# - `yyyy-MM-dd` (Year-Month-Day)
# - `MMMM dd, yyyy` (Month Day, Year)
# - `E, MMM dd yyyy` (Day, Month Day Year)


# CELL ********************

df_formatted = df.select(
    col("Date"),
    date_format(col("Date"), "yyyy-MM-dd").alias("Formatted_YYYY_MM_DD"),
    date_format(col("Date"), "MMMM dd, yyyy").alias("Formatted_Long"),
    date_format(col("Date"), "E, MMM dd yyyy").alias("Formatted_Short")
)
df_formatted.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

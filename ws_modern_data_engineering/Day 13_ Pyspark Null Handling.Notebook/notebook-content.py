# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # Null Handling in PySpark

# CELL ********************

# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnull, coalesce, lit

# Create a Spark session
spark = SparkSession.builder.appName("NullHandling").getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Sample Sales Data with Null Values

# CELL ********************

# Sample data: sales data with nulls
data = [
    ("John", "North", 100, None),
    ("Doe", "East", None, 50),
    (None, "West", 150, 30),
    ("Alice", None, 200, 40),
    ("Bob", "South", None, None),
    (None, None, None, None)
]
columns = ["Name", "Region", "UnitsSold", "Revenue"]

# Create DataFrame
df = spark.createDataFrame(data, columns)
df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. Detecting Null Values

# CELL ********************

# Identify null values in specific columns
df.select(col("Name"), col("Region"), col("UnitsSold"), col("Revenue"),
          col("Name").isNull().alias("Name_is_null"),
          col("Region").isNull().alias("Region_is_null"),
          col("UnitsSold").isNull().alias("UnitsSold_is_null"),
          col("Revenue").isNull().alias("Revenue_is_null")
         ).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Dropping Rows with Null Values

# CELL ********************

# Drop rows containing null values in any column
df_drop_any = df.dropna()
df_drop_any.show()

# Drop rows only if all columns contain null values
df_drop_all = df.dropna(how='all')
df_drop_all.show()

# Drop rows where specific columns contain null values
df_drop_subset = df.dropna(subset=["Name", "UnitsSold"])
df_drop_subset.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Filling Null Values

# CELL ********************

# Fill null values in specific columns
df_fill = df.fillna({"Region": "Unknown", "UnitsSold": 0, "Revenue": 0})
df_fill.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Using Coalesce to Handle Nulls in Aggregations

# CELL ********************

# Using coalesce to replace nulls with fallback values
df_coalesce = df.select(
    col("Name"),
    col("Region"),
    coalesce(col("UnitsSold"), lit(0)).alias("UnitsSold_Filled"),
    coalesce(col("Revenue"), lit(0)).alias("Revenue_Filled")
)
df_coalesce.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Summary of Null Handling in PySpark
# 1. **Detecting Nulls**: Use `isNull()` to identify missing values.
# 2. **Dropping Nulls**: Use `dropna()` to remove rows containing nulls.
# 3. **Filling Nulls**: Use `fillna()` to replace nulls with default values.
# 4. **Coalesce Function**: Use `coalesce()` to provide fallback values in case of nulls.
# 5. **Handling Aggregations**: Use `coalesce()` in aggregation functions to avoid null impact.

# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # PySpark Data Manipulation Guide
# 
# ## 1. Changing Data Types (Schema Transformation)
# Learn how to modify column data types using `cast()`:


# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# creatin sample DataFrame
data = [
    ("John", 35, "IT", 50000),
    ("Alice", 40, "IT", 60000),
    ("Bob", None, "HR", 45000),
    ("John", 35, "IT", 50000),
    ("Alice", 40, "IT", 60000), 
    ("Charlie", 30, "HR", 55000),
    ("David", 45, "Finance", 70000),
    ("Eve", 28, "Finance", 65000)
]
col = ["Name", "Age", "Department", "Salary"]

df = spark.createDataFrame(data,col)

df.printSchema()

#Changing datatypes
df = df.withColumn("Age", df.Age.cast("double")) \
       .withColumn("Salary", df.Salary.cast("string"))

df.printSchema()

#Approach 2
df = df.selectExpr(
    "Name", 
    "CAST(Age AS DOUBLE) AS Age",
    "CAST(Department AS String) AS Department",
    "CAST(Salary AS String) AS Salary"
)
                   
df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Filtering Data
# Examples of basic filtering operations:

# CELL ********************

# Basic filtering
filtered_df = df.filter(df.Salary > 50000)
filtered_df.show()

#or

filtered_df2 = df.where(df.Salary > 50000)
filtered_df2.show()


# Filter non-null values
filtered_notnull_df = df.filter(df["Name"].isNotNull())
filtered_notnull_df.show()

# Filterning null values
filtered_null_df = df.filter(df["Age"].isNull())
filtered_null_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Multiple Filters
# Combining multiple conditions:

# CELL ********************

# Multiple conditions
filtered_df = df.filter(
    (df.Age==35 ) & 
    (df.Salary>=50000))
display(filtered_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Working with Distinct Values
# Handling duplicate data:

# CELL ********************

# Get distinct rows
distinct_df = df.distinct()
distinct_df.show()

# Get distinct values from specific column
distinct_names = df.select("Name").distinct()
distinct_names.show()

# Remove duplicates based on multiple columns (using "Name" and "Department" as an example)
unique_df = df.dropDuplicates(["Name", "Department"])
unique_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Counting Distinct Values
# Analyzing unique value counts:

# CELL ********************

# Count distinct names
distinct_name_count = df.select("Name").distinct().count()
print(f"Distinct names: {distinct_name_count}")

# Count distinct combinations of Name and Department
distinct_combo_count = df.select("Name", "Department").distinct().count()
print(f"Distinct name-department combinations: {distinct_combo_count}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "52c67a67-0dfc-4689-8fef-12b1d8c947b1",
# META       "default_lakehouse_name": "lh_data_engineering",
# META       "default_lakehouse_workspace_id": "0ab0bc35-a379-49b7-9165-d79f3654ac47"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # PySpark DataFrame Manipulation
# ## Part 2: Adding, Renaming, and Dropping Columns
# 
# In this notebook, we will cover the following topics:
# * Adding new columns to a DataFrame using `withColumn()`
# * Renaming columns using `withColumnRenamed()`
# * Dropping columns using `drop()`
# * Immutability of DataFrames in Spark
# 
# ### Adding New Columns with `withColumn()`
# In PySpark, the `withColumn()` function is widely used to add new columns to a DataFrame. You can either assign a constant value using `lit()` or perform transformations using existing columns.


# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, expr, col

# Create Spark session
spark = SparkSession.builder.appName("ColumnManipulation").getOrCreate()

# Create sample data
data = [
    ("John", 30, "Sales", 50000),
    ("Lisa", 25, "Marketing", 45000),
    ("Mike", 35, "Engineering", 60000)
]

# Create DataFrame
df = spark.createDataFrame(data, ["Name", "Age", "Department", "Salary"])
df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. Adding New Columns
# Demonstrate different ways to add columns using `withColumn()`:

# CELL ********************

# Add a constant value column
df_with_bonus = df.withColumn("Bonus", lit(5000))

# Add a calculated column
df_with_total = df_with_bonus.withColumn("TotalComp", col("Salary") + col("Bonus"))

# Add a column with conditional logic
df_with_category = df_with_total.withColumn(
    "SalaryCategory",
    expr("CASE WHEN Salary >= 50000 THEN 'High' ELSE 'Standard' END")
)

df_with_category.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Renaming Columns
# Examples of renaming columns:

# CELL ********************

# Rename single column
df_renamed = df_with_category.withColumnRenamed("Department", "Division")

# Rename multiple columns using multiple withColumnRenamed calls
df_final = df_renamed \
    .withColumnRenamed("TotalComp", "TotalCompensation") \
    .withColumnRenamed("SalaryCategory", "CompLevel")

df_final.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Dropping Columns
# Examples of removing columns:

# CELL ********************

# Drop a single column
df_dropped = df_final.drop("Bonus")

# Drop multiple columns
df_minimal = df_dropped.drop("CompLevel", "TotalCompensation")

df_minimal.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Verify DataFrame Immutability
# Demonstrate that original DataFrame remains unchanged:

# CELL ********************

print("Original DataFrame:")
df.show()

print("\nFinal Modified DataFrame:")
df_minimal.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

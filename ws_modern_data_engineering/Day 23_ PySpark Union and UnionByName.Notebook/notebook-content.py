# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # PySpark Union and UnionByName
# 
# - **`union()`**: Requires both DataFrames to have the same number of columns and order.
# - **`unionByName()`**: Matches columns by name, even if the order is different.


# CELL ********************


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create Spark session
spark = SparkSession.builder.appName("PySparkUnionExample").getOrCreate()

# Define schemas
schema1 = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True)
])

schema2 = StructType([
    StructField("id", IntegerType(), True),
    StructField("department", StringType(), True),
    StructField("name", StringType(), True)
])

# Create DataFrames
data1 = [(1, "Alice", "HR"), (2, "Bob", "IT")]
data2 = [(3, "Charlie", "Finance"), (4, "David", "Marketing")]

df1 = spark.createDataFrame(data1, schema=schema1)
df2 = spark.createDataFrame(data2, schema=schema2)

# Show original DataFrames
print("DataFrame 1:")
df1.show()

print("DataFrame 2:")
df2.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Using `union()`
# `union()` requires that both DataFrames have the same schema (column names and order). If they don’t, an error will be thrown.

# CELL ********************


# Union operation (Requires same schema)
df_union = df1.union(df2)
df_union.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Using `unionByName()`
# `unionByName()` allows us to union DataFrames with different column orders by matching column names.

# CELL ********************


# UnionByName operation (Matches columns by name)
df_union_by_name = df1.unionByName(df2)
df_union_by_name.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Summary
# - Use `union()` when DataFrames have identical schemas.
# - Use `unionByName()` when columns have the same names but are in a different order.
# - Neither function removes duplicates, so `distinct()` may be needed if necessary.


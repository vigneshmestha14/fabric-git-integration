# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # PySpark Union vs Union All

# MARKDOWN ********************

# ## Key Differences

# MARKDOWN ********************

# - `union()`: Removes duplicate rows when combining DataFrames.

# MARKDOWN ********************

# - `unionAll()`: Retains all duplicate rows when combining DataFrames (deprecated, use `union()`).

# CELL ********************

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('UnionExample').getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import Row

data1 = [("Alice", 1), ("Bob", 2)]
data2 = [("Bob", 2), ("Cathy", 3), ("David", 4)]
columns = ["Name", "Id"]


df1 = spark.createDataFrame(data1,columns)
df2 = spark.createDataFrame(data2,columns)

df1.show()
df2.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Union
df_union = df1.union(df2)
df_union.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### The union() function combines DataFrames but does NOT remove duplicates explicitly. If you want unique values, use 'distinct()' on the result.


# CELL ********************

df_union_distinct = df_union.distinct()
df_union_distinct.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Conclusion 
# - Use `union()` to merge DataFrames and remove duplicates with `.distinct()` if needed.
# - `unionAll()` is deprecated, and `union()` is the recommended approach.

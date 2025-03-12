# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# 
# # Pyspark Joins

# CELL ********************


# PySpark Joins Demonstration

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark Session
spark = SparkSession.builder.appName("PySparkJoins").getOrCreate()

# Sample DataFrames
data1 = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
data2 = [(1, "HR"), (2, "IT"), (4, "Finance")]

columns1 = ["ID", "Name"]
columns2 = ["ID", "Department"]

df1 = spark.createDataFrame(data1, columns1)
df2 = spark.createDataFrame(data2, columns2)

df1.show()
df2.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# # 1. Inner Join 
# Returns Matching records only

# CELL ********************

inner_join_df = df1.join(df2, "ID", "inner")
inner_join_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 2. Left Join 
# Keep all records from df1

# CELL ********************

left_join_df = df1.join(df2, "ID", "left")
left_join_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# # 3. Right Join  
# Keep all records from df2

# CELL ********************

right_join_df = df1.join(df2, "ID", "right")
right_join_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# # 4. Full Outer Join
# Keep all records from both DataFrames

# CELL ********************

full_outer_join_df = df1.join(df2, "ID", "outer")
full_outer_join_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# # 5. Left Semi Join 
# Return only matching records from df1

# CELL ********************

left_semi_df = df1.join(df2, "ID", "left_semi")
left_semi_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# # 6. Left Anti Join 
# Return only non-matching records from df1

# CELL ********************

left_anti_df = df1.join(df2, "ID", "left_anti")
left_anti_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# # 7. Cross Join 
# Cartesian product of both DataFrames

# CELL ********************

cross_join_df = df1.crossJoin(df2)
cross_join_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 8. Join with Explicit Conditions (Different Column Names)

# CELL ********************

df1 = df1.withColumnRenamed("ID", "ID1")
df2 = df2.withColumnRenamed("ID", "ID2")

explicit_join_df = df1.join(df2, df1.ID1 == df2.ID2, "inner").select("ID1", "Name", "Department")
explicit_join_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

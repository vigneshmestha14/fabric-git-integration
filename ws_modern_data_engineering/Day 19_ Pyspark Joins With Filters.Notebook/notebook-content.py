# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # **Pyspark Joins With Filters**

# CELL ********************

# Import necessary libraries
from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder.appName("JoinsWithFilter").getOrCreate()

# Sample DataFrames
data1 = [(1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 35)]
data2 = [(1, "HR"), (2, "Finance"), (4, "IT")]

df1 = spark.createDataFrame(data1, ["id", "name", "age"])
df2 = spark.createDataFrame(data2, ["id", "department"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 1. Inner Join with Filter (Only employees under 30)

# CELL ********************

inner_join = df1.join(df2, "id", "inner").filter(df1.age < 30)
inner_join.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 2. Left Join with Filter (Show employees who have NO department)

# CELL ********************

left_join = df1.join(df2, "id", "left").filter(df2.department.isNull())
left_join.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 3. Right Join with Filter (Show departments that have employees)

# CELL ********************

right_join = df1.join(df2, "id", "right").filter(df1.name.isNotNull())
right_join.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 4. Full Outer Join with Filter (Show rows where there was NO match)

# CELL ********************

full_outer_join = df1.join(df2, "id", "outer").filter(df1.name.isNull() | df2.department.isNull())
full_outer_join.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 5. Left Semi Join with Filter (Show employees with department but under 30)

# CELL ********************

left_semi_join = df1.join(df2, "id", "left_semi").filter(df1.age < 30)
left_semi_join.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 6. Left Anti Join with Filter (Show employees with NO department and age > 30)

# CELL ********************

left_anti_join = df1.join(df2, "id", "left_anti").filter(df1.age > 30)
left_anti_join.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 7. Cross Join with Filter (Pair employees with departments but only if age < 30)

# CELL ********************

cross_join = df1.crossJoin(df2).filter(df1.age < 30)
cross_join.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

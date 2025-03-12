# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# 
# # Casting and printSchema in PySpark
# 
# In PySpark, we often need to change the data types of columns in a DataFrame using the `cast()` function.
# The `printSchema()` function is used to display the structure of a DataFrame, including column names and data types.

# CELL ********************


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create a Spark session
spark = SparkSession.builder.appName("CastingExample").getOrCreate()

# Define schema
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", StringType(), True),
    StructField("salary", StringType(), True)
])

# Sample data
data = [("Alice", "30", "50000"), ("Bob", "40", "60000"), ("Charlie", "35", "70000")]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Print schema before casting
df.printSchema()

# Show data
df.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# ## 2. Casting Data Types
# 
# We will now cast the `age` and `salary` columns from `StringType` to `IntegerType`.


# CELL ********************


from pyspark.sql.functions import col

# Cast columns to IntegerType
df_casted = df.withColumn("age", col("age").cast(IntegerType()))\
               .withColumn("salary", col("salary").cast(IntegerType()))

# Print schema after casting
df_casted.printSchema()

# Show transformed data
df_casted.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# ## 3. Conclusion
# 
# - The `printSchema()` function allows us to check the structure of a DataFrame.
# - The `cast()` function is used to change column data types.
# - Casting is useful when reading data where numerical values are stored as strings.
# 
# This demonstrates how to properly convert data types in PySpark!


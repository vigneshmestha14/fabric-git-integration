# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # Aggregate Function in DataFrame – Part 2

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create Spark session
spark = SparkSession.builder.appName('AggregationExamples').getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Sample Data Creation

# CELL ********************

data = [
    ('HR', 10000, 500, 'John'),
    ('Finance', 20000, 1500, 'Doe'),
    ('HR', 15000, 1000, 'Alice'),
    ('Finance', 25000, 2000, 'Eve'),
    ('HR', 20000, 1500, 'Mark')
]

schema = StructType([
    StructField('department', StringType(), True),
    StructField('salary', IntegerType(), True),
    StructField('bonus', IntegerType(), True),
    StructField('employee_name', StringType(), True)
])

df = spark.createDataFrame(data, schema)
df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. Grouped Aggregation
# - **sum()**: Adds values in a group.
# - **avg()**: Computes average.
# - **max()**, **min()**: Finds max/min values.

# CELL ********************

df.groupBy('department').agg(
   sum('salary').alias('total_salary'),
   avg('salary').alias('avg_salary'),
   max('bonus').alias('max_bonus'),
   min('bonus').alias('min_bonus')
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Multiple Aggregations
# Perform multiple aggregations in a single step.

# CELL ********************

df.groupBy('department').agg(
    count('salary').alias('count_salary'),
    avg('bonus').alias('avg_bonus'),
    max('salary').alias('max_salary')
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Concatenate Strings

# CELL ********************

df.groupBy('department').agg(
    concat_ws(', ', collect_list('employee_name')).alias('employees')
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. First and Last Values

# CELL ********************

df.groupBy('department').agg(
    first('employee_name').alias('first_employee'),
    last('employee_name').alias('last_employee')
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Standard Deviation and Variance

# CELL ********************

df.select(
   stddev('salary').alias('stddev_salary'),
   variance('salary').alias('variance_salary')
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Aggregation with Alias

# CELL ********************

df.groupBy('department').agg(
    sum('salary').alias('Total Salary')
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Sum of Distinct Values

# CELL ********************

df.select(
    sumDistinct('salary').alias('sum_distinct_salary')
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

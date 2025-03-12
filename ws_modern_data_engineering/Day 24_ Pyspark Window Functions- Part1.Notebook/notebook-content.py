# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # Window Functions in PySpark
# Window functions allow you to perform calculations across a set of rows related to the current row within a specified partition. Unlike `groupBy`, window functions do not reduce the number of rows but calculate a value for each row.

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Creating a Spark Session

# CELL ********************

spark = SparkSession.builder.appName('WindowFunctionsExample').getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Creating a Sample DataFrame
# Let's create a sample dataset to demonstrate window functions.

# CELL ********************

data = [
    ('A', 'X', 1, '2023-01-01'),
    ('A', 'X', 2, '2023-01-02'),
    ('A', 'Y', 3, '2023-01-01'),
    ('A', 'Y', 3, '2023-01-02'),
    ('B', 'X', 5, '2023-01-01'),
    ('B', 'X', 4, '2023-01-02'),
]

columns = ['category', 'sub_category', 'value', 'timestamp']
df = spark.createDataFrame(data, columns)
df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Defining a Window Specification
# A window specification determines how the rows are partitioned and ordered.

# CELL ********************

window_spec = Window.partitionBy('category', 'sub_category')\
                     .orderBy(F.col('timestamp'), F.col('value'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Applying Window Functions
# ### 1. Row Number
# Assigns a unique integer to each row within the partition.

# CELL ********************

df = df.withColumn('row_number', F.row_number().over(window_spec))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 2. Rank
# Assigns the same rank to rows with the same values in the order criteria, but the next rank has a gap.

# CELL ********************

df = df.withColumn('rank', F.rank().over(window_spec))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 3. Dense Rank
# Similar to rank(), but does not leave gaps in ranking.

# CELL ********************

df = df.withColumn('dense_rank', F.dense_rank().over(window_spec))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 4. Lead and Lag Functions
# - `lead()` returns the value of the next row within the window.
# - `lag()` returns the value of the previous row.

# CELL ********************

df = df.withColumn('next_value', F.lead('value').over(window_spec))
df = df.withColumn('previous_value', F.lag('value').over(window_spec))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 5. Aggregation Functions
# Window functions can also be used to compute aggregated values over a specified window.

# CELL ********************

df = df.withColumn('avg_value', F.avg('value').over(window_spec))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Displaying Final Results

# CELL ********************

df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

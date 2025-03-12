# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # Window Functions in PySpark - Part 2

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Creating a Spark Session

# CELL ********************

spark = SparkSession.builder.appName('WindowFunctionsPart2').getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Creating a Sample DataFrame

# CELL ********************

data = [
    ('Alice', 100),
    ('Bob', 200),
    ('Charlie', 200),
    ('David', 300),
    ('Eve', 400),
    ('Frank', 500),
    ('Grace', 500),
    ('Hank', 600),
    ('Ivy', 700),
    ('Jack', 800)
]

columns = ['Name', 'Score']
df = spark.createDataFrame(data, columns)
df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Defining a Window Specification
# A window specification is created by ordering rows based on the 'Score' column.

# CELL ********************

window_spec = Window.orderBy('Score')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Applying Window Functions
# ### 1. Rank
# Assigns the same rank to rows with the same score, with gaps for the next rank.

# CELL ********************

df1 = df.withColumn('Rank', F.rank().over(window_spec))
df1.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 2. Dense Rank
# Similar to `rank()`, but does not leave gaps between ranks.

# CELL ********************

df2 = df.withColumn('DenseRank', F.dense_rank().over(window_spec))
df2.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 3. Row Number
# Assigns a unique row number to each record.

# CELL ********************

df3 = df.withColumn('RowNumber', F.row_number().over(window_spec))
df3.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 4. Lead Function - Score Difference with Next Row
# Calculates the difference between the current and next row's score.

# CELL ********************

df4 = df.withColumn('ScoreDifferenceWithNext', F.lead('Score').over(window_spec) - df['Score'])
df4.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 5. Lag Function - Score Difference with Previous Row
# Calculates the difference between the current and previous row's score.

# CELL ********************

df5 = df.withColumn('ScoreDifferenceWithPrevious', df['Score'] - F.lag('Score').over(window_spec))
df5.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # Pivot in PySpark
# The pivot operation in PySpark is used to transpose rows into columns based on a specified column's unique values. It's useful for creating wide-format data where values in one column become new column headers, and corresponding values from another column fill those headers.


# MARKDOWN ********************

# ## Key Concepts
# - **groupBy and pivot:** Used together to group data and pivot a column into new headers.
# - **Aggregation Function:** Functions like `sum`, `avg`, `count` fill the values in pivoted columns.
# - **Performance Considerations:** Pivoting can be expensive, especially with many unique values. Explicitly specifying pivot values improves performance.


# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Initialize Spark Session
spark = SparkSession.builder.appName('PivotExample').getOrCreate()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Sample Data
data = [
    ('Product A', 'North', 100),
    ('Product A', 'South', 150),
    ('Product B', 'North', 200),
    ('Product B', 'South', 250),
    ('Product C', 'North', 300),
]

columns = ['Product', 'Region', 'Sales']
df = spark.createDataFrame(data, columns)
df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Pivot DataFrame
pivot_df = df.groupBy('Product').pivot('Region').agg(sum('Sales'))
pivot_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Explanation of Code
# - **groupBy('Product')**: Groups data by the `Product` column.
# - **pivot('Region')**: Converts unique `Region` values (North, South) into new columns.
# - **agg(sum('Sales'))**: Computes the sum of `Sales` for each combination of `Product` and pivoted columns.


# MARKDOWN ********************

# ### Notes
# - **Explicit Pivot Values:** To optimize performance, specify pivot values explicitly:
#   ```python
#   pivot_df = df.groupBy('Product').pivot('Region', ['North', 'South']).agg(sum('Sales'))
#   ```
# - **Handling Nulls:** If no data exists for a pivoted value, the resulting cell will be `null`.
# - **Other Aggregations:** Functions like `avg`, `max`, and `min` can also be used.


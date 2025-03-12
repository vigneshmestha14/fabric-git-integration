# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # Explode vs Explode Outer in PySpark
# In PySpark, `explode` and `explode_outer` are functions used to work with nested data structures like arrays or maps by **flattening** each element into separate rows. The key difference between them lies in how they handle null or empty arrays.

# MARKDOWN ********************

# ## 1. explode()
# **`explode()`** takes an array or map column and creates a new row for each element.
# **If the array is empty or null, it drops the row entirely.**

# MARKDOWN ********************

# ### Key Characteristics:
# - Converts each element in an array (or key-value pair in a map) into its own row.
# - **Drops** rows where the array is empty (`[]`) or null (`None`).

# MARKDOWN ********************

# ### Syntax:
# ```python
# from pyspark.sql.functions import explode
# df.select(explode(df['column_with_array'])).show()
# ```

# MARKDOWN ********************

# ### Example of `explode()`

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

# Initialize Spark session
spark = SparkSession.builder.appName('ExplodeExample').getOrCreate()

# Sample DataFrame with arrays
data = [
    ('Alice', ['Math', 'Science']),
    ('Bob', ['History']),
    ('Cathy', []),  # Empty array
    ('David', None)  # Null array
]

df = spark.createDataFrame(data, ['Name', 'Subjects'])
df.show()

# Use explode to flatten the array
exploded_df = df.select('Name', explode('Subjects').alias('Subject'))
exploded_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Explanation:
# - `explode()` expands the `Subjects` array into individual rows.
# - **Rows with empty (`[]`) or null (`None`) arrays are removed**, so Cathy and David do not appear in the output.

# MARKDOWN ********************

# ---
# ## 2. explode_outer()
# **`explode_outer()`** works similarly to `explode()`, but it **keeps rows** with null or empty arrays, filling them with `null` values in the resulting column.

# MARKDOWN ********************

# ### Key Characteristics:
# - Converts each element in an array or each entry in a map into its own row.
# - **Retains** rows where the array is empty (`[]`) or null (`None`), filling them with `null`. 

# MARKDOWN ********************

# ### Syntax:
# ```python
# from pyspark.sql.functions import explode_outer
# df.select(explode_outer(df['column_with_array'])).show()
# ```

# MARKDOWN ********************

# ### Example of `explode_outer()`

# CELL ********************

from pyspark.sql.functions import explode_outer

# Use explode_outer to retain null and empty arrays
exploded_outer_df = df.select('Name', explode_outer('Subjects').alias('Subject'))
exploded_outer_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Explanation:
# - `explode_outer()` expands the `Subjects` array into individual rows.
# - **Unlike `explode()`, rows with empty (`[]`) or null (`None`) arrays are kept** with `null` values in the `Subject` column.

# MARKDOWN ********************

# ---
# ## Summary Table of Differences
# 
# | Function         | Description | Null/Empty Arrays Behavior |
# |-----------------|-------------|-----------------------------|
# | `explode()`     | Expands each element of an array or map into individual rows | Drops rows with null or empty arrays |
# | `explode_outer()` | Similar to `explode()`, but retains rows with null or empty arrays | Keeps rows with null or empty arrays, filling with `null` |
# 
# These functions are useful when working with complex nested data structures, especially when dealing with JSON or other hierarchical data.

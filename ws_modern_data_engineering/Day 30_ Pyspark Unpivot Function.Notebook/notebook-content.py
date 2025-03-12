# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # Unpivot in PySpark
# 
# The **unpivot** operation is used to transform wide-format data into a long-format table. This is useful when you need to restructure data for analysis.

# MARKDOWN ********************

# ## Sample Data
# 
# We start with a DataFrame where sales data is stored in separate columns for different regions (North, South, East, West).

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# Initialize Spark Session
spark = SparkSession.builder.appName('UnpivotExample').getOrCreate()

# Sample Data
data = [
    ('Product A', 100, 200, 150, 180),
    ('Product B', 90, 210, 160, 190),
    ('Product C', 120, 180, 140, 170)
]

columns = ['Product', 'North', 'South', 'East', 'West']
df = spark.createDataFrame(data, columns)

df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Unpivoting Data
# 
# We use the **stack()** function inside **selectExpr()** to transform the regional columns into rows. Each region name will now be stored in a single column (`Region`), and the corresponding sales values will be in another column (`Sales`).

# CELL ********************

unpivot_expr = "stack(4, 'North', North, 'South', South, 'East', East, 'West', West) as (Region, Sales)"
df_unpivoted = df.selectExpr('Product', unpivot_expr)

df_unpivoted.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Conclusion
# 
# - **Unpivoting helps in data normalization**, making it easier to analyze and visualize.
# - The `stack()` function is an efficient way to perform this transformation in PySpark.
# - If the column names are dynamic, you can retrieve them using `df.columns` and construct the `selectExpr` dynamically.

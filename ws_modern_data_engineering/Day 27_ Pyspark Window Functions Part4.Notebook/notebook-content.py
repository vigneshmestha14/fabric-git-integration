# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# ## Window Functions in PySpark - Part 4

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark = SparkSession.builder.appName('WindowFunctionsPart4').getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

emp_data = [
    (1, 'Alice', 1, 6300),
    (2, 'Bob', 1, 6200),
    (3, 'Charlie', 2, 7000),
    (4, 'David', 2, 7200),
    (5, 'Eve', 1, 6300),
    (6, 'Frank', 2, 7100)
]
dept_data = [
    (1, 'HR'),
    (2, 'Finance')
]

emp_df = spark.createDataFrame(emp_data, ['EmpId', 'EmpName', 'DeptId', 'Salary'])
dept_df = spark.createDataFrame(dept_data, ['DeptId', 'DeptName'])
emp_df.show()
dept_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Finding the Highest Salary in Each Department
# We use a window function to rank salaries within each department.

# CELL ********************

window_spec = Window.partitionBy('DeptId').orderBy(F.desc('Salary'))
ranked_salary_df = emp_df.withColumn('Rank', F.rank().over(window_spec))
ranked_salary_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Filtering the Top Salary in Each Department

# CELL ********************

result_df = ranked_salary_df.filter(F.col('Rank') == 1)
result_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Joining with Department Names

# CELL ********************

result_df = result_df.join(dept_df, ['DeptId'], 'left')
result_df.select('EmpName', 'DeptName', 'Salary').show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

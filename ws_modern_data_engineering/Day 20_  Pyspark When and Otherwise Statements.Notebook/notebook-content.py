# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # When and Otherwise Statements in PySpark

# CELL ********************

data = [
    ('John', 'HR', 4000),
    ('Alice', 'IT', 7000),
    ('Bob', 'Finance', 5000),
    ('Eve', 'IT', 8000),
    ('Charlie', 'HR', 4500)
]
schema = ['Name', 'Department', 'Salary']
df = spark.createDataFrame(data, schema=schema)
df.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 1. Basic Analysis with When-Otherwise

# CELL ********************

from pyspark.sql.functions import col, when, avg, sum

# Add salary category based on conditions
df_with_category = df.withColumn("SalaryCategory",
    when(col("Salary") <= 4000, "Entry Level")
    .when((col("Salary") > 4000) & (col("Salary") <= 6000), "Mid Level")
    .otherwise("Senior Level")
)

df_with_category.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 2. Department-wise Analysis

# CELL ********************

# Calculate department statistics
dept_stats = df.groupBy("Department").agg(
    avg("Salary").alias("AvgSalary"),
    sum("Salary").alias("TotalSalary")
)

dept_stats.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 3. Conditional Aggregations

# CELL ********************

# Count employees by salary ranges per department
salary_distribution = df.groupBy("Department").agg(
    sum(when(col("Salary") <= 5000, 1).otherwise(0)).alias("Junior_Count"),
    sum(when(col("Salary") > 5000, 1).otherwise(0)).alias("Senior_Count")
)

salary_distribution.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 4. Complex Transformations

# CELL ********************

# Create detailed employee analysis
detailed_analysis = df.withColumn(
    "Performance_Bonus",
    when(col("Department") == "IT", col("Salary") * 0.15)
    .when(col("Department") == "HR", col("Salary") * 0.10)
    .otherwise(col("Salary") * 0.12)
).withColumn(
    "Department_Rank",
    when(col("Department") == "IT", "High")
    .when(col("Department") == "Finance", "Medium")
    .otherwise("Standard")
)

detailed_analysis.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 5. Window Functions with Conditions

# CELL ********************

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, dense_rank

# Create window specification
window_spec = Window.partitionBy("Department").orderBy(col("Salary").desc())

# Add rankings
ranked_df = df.withColumn("Dept_Rank", rank().over(window_spec))\
    .withColumn("Status",
        when(col("Dept_Rank") == 1, "Top Performer")
        .when(col("Dept_Rank") == 2, "Strong Performer")
        .otherwise("Regular")
    )

ranked_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 6. Multiple Column Conditions

# CELL ********************

# Complex categorization based on multiple columns
categorized_df = df.withColumn("Employee_Category",
    when((col("Department") == "IT") & (col("Salary") > 6000), "Senior IT")
    .when((col("Department") == "HR") & (col("Salary") > 4000), "Senior HR")
    .when((col("Department") == "Finance") & (col("Salary") > 5000), "Senior Finance")
    .otherwise("Junior Staff")
)

categorized_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

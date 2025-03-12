# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# ## Trim Function in DataFrame

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, ltrim, rtrim, rpad, lpad, trim, col

# Create a Spark session
spark = SparkSession.builder.appName("PySparkTrimFunctions").getOrCreate()

# Sample employee data with leading and trailing spaces in the 'Name' column
data = [
    (1, " Alice ", "HR"),
    (2, " Bob", "IT"),
    (3, "Charlie ", "Finance"),
    (4, " David ", "HR"),
    (5, "Eve ", "IT")
]

# Define the schema for the DataFrame
columns = ["EmployeeID", "Name", "Department"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Show the original DataFrame
df.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Applying Trimming and Padding Functions
# 
# ### 1. `ltrim()`, `rtrim()`, and `trim()`
# - **`ltrim()`**: Removes leading spaces.
# - **`rtrim()`**: Removes trailing spaces.
# - **`trim()`**: Removes both leading and trailing spaces.


# CELL ********************

df_trimmed = df.select(
    col("EmployeeID"),
    col("Department"),
    ltrim(col("Name")).alias("ltrim_Name"),  # Remove leading spaces
    rtrim(col("Name")).alias("rtrim_Name"),  # Remove trailing spaces
    trim(col("Name")).alias("trim_Name")     # Remove both leading and trailing spaces
)

df_trimmed.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 2. `lpad()` and `rpad()`
# - **`lpad()`**: Pads the left side of a string with a specified character up to a certain length.
# - **`rpad()`**: Pads the right side of a string with a specified character up to a certain length.


# CELL ********************

df_padded = df.select(
    col("EmployeeID"),
    col("Department"),
    lpad(col("Name"), 10, "X").alias("lpad_Name"),  # Left pad with 'X' to make the string length 10
    rpad(col("Name"), 10, "Y").alias("rpad_Name")   # Right pad with 'Y' to make the string length 10
)

df_padded.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Output Explanation:
# - **`ltrim_Name`**: The leading spaces from the `Name` column are removed.
# - **`rtrim_Name`**: The trailing spaces from the `Name` column are removed.
# - **`trim_Name`**: Both leading and trailing spaces are removed from the `Name` column.
# - **`lpad_Name`**: The `Name` column is padded on the left with `'X'` until the string length becomes `10`.
# - **`rpad_Name`**: The `Name` column is padded on the right with `'Y'` until the string length becomes `10`.


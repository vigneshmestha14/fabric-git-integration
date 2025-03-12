# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "52c67a67-0dfc-4689-8fef-12b1d8c947b1",
# META       "default_lakehouse_name": "lh_data_engineering",
# META       "default_lakehouse_workspace_id": "0ab0bc35-a379-49b7-9165-d79f3654ac47"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # PySpark DataFrame Schema Definition

# MARKDOWN ********************

# ### 1. Defining Schema Programmatically with StructType

# CELL ********************

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType

schema = StructType([
    StructField("SalesOrderNumber", StringType(), True),
    StructField("SalesOrderLineNumber", IntegerType(), True),
    StructField("OrderDate", DateType(), True),
    StructField("CustomerName", StringType(), True),
    StructField("Email", StringType(), True),
    StructField("Item", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("UnitPrice", FloatType(), True),
    StructField("Tax", FloatType(), True)
])

df = spark.read.csv("Files/orders/*.csv", schema=schema)
df.printSchema()
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 2. Defining Schema as a String

# CELL ********************

schema = '''SalesOrderNumber string,
            SalesOrderLineNumber INT,
            OrderDate DATE,
            CustomerName STRING,
            Email STRING,
            Item String,
            Quantity INT,
            UnitPrice FLOAT,
            Tax FLOAT'''

df = spark.read.csv("Files/orders/*.csv",schema=schema)
df.printSchema()
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Explanation
# 
# - **Schema Definition**: Both methods define a schema for the DataFrame, accommodating the dataset's requirements, including handling null values where applicable.
# - **Data Types**: The `Joining_Date` column is defined as `StringType` to accommodate potential date format issues or missing values.
# - **Loading the DataFrame**: The `spark.read.load` method is used to load the CSV file into a DataFrame using the specified schema.
# - **Printing the Schema**: The `df.printSchema()` function allows you to verify that the DataFrame is structured as intended.

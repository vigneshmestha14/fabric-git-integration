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

# # Loading Data from CSV File into a DataFrame

# MARKDOWN ********************

# Loading data into DataFrames is a fundamental step in any data processing workflow in PySpark. This document outlines how to load data from CSV files into a DataFrame, including using a custom schema and the implications of using the inferSchema option.

# MARKDOWN ********************

# ## Step-by-Step Guide

# MARKDOWN ********************

# 1. Import Required Libraries
# 2. Define the Schema
# 3. Read the CSV File
# 4. Load Multiple CSV Files
# 5. Display the DataFrame

# CELL ********************

# Import Required Libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Define the Schema

# CELL ********************

# Define the schema using StructType
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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Read the CSV File

# CELL ********************

# Read the CSV File
df = spark.read.csv("Files/Pyspark_files/2019.csv", schema=schema, header=True)
df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Load Multiple CSV Files

# CELL ********************

# Define the common path and file paths
common_path = 'Files/Pyspark_files/'
file_paths = [
    f"{common_path}2019.csv",
    f"{common_path}2020.csv",
    f"{common_path}2021.csv"
]

# Load the CSV files into a Spark DataFrame
df_multiple = spark.read.csv(file_paths, schema=schema, header=True)

# Display the resulting DataFrame
display(df_multiple)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Display the DataFrame

# CELL ********************

# Display the DataFrame
print("Schema of the DataFrame:")
df.printSchema()
print("First 20 rows of the DataFrame:")
display(df)
df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

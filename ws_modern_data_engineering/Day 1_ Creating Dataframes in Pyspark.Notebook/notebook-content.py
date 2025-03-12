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

# # Pyspark: SQL and DataFrames

# MARKDOWN ********************

# ###### Spark SQL: A Spark module for working with structured data using SQL.                       
# ###### DataFrame: A distributed collection of data oraganized into name column, same as table in RDBMS

# MARKDOWN ********************

# 1. Creating DataFrame Manually

# CELL ********************

# Sample Data
data = [(1,"Alice"),(2,"Bob")
        ,(3,"Charlie"),(4,"Dave"),
        (5,"Eve"),(6,"Frank"),
        (7,"Grace"),(8,"Harry"),
        (9,"Ivan"),(10,"Jose")]

columns = ["id","name"]

#Create DatFrame
df = spark.createDataFrame(data,columns)

# Display DataFrame in table structure
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# show() Function in Pyspark DataFrame

# MARKDOWN ********************

# The show() function in Pyspark displays the content of a DataFrame in a tabular Format. It has several useful parameters for customization
# 
#   1. n:number of rows to display (default is 20)
#   2. truncate: If set to True, it truncates column values longer than 20 characters(bydefault is True)
#   3. vertical: Prints rows in vertically is set to True

# CELL ********************

#Show the first 3 rows, truncate columns to 25 charactersand displays vertically
df.show(3,truncate=3,vertical=True)

# Show entire DataFrame
df.show()

# Show first 5 rows
df.show(5)

# Show DataFrame without truncatingany columns
df.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 2. Create DataFrame from Pandas

# CELL ********************

import pandas as pd

#Sample DataFrame
pandas_df = pd.DataFrame(data=data,columns=columns)

# Convert to Pyspark DataFrame
df_to_pyspark = spark.createDataFrame(pandas_df)
df_to_pyspark.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 3. Creating DataFrame from Dictionary

# CELL ********************

dict_data = [{'id':1, 'Name':'Alice'},
             {'id':2, 'Name':'Bob'},
             {'id':3, 'Name':'Charlie'}]
df = spark.createDataFrame(dict_data)
df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 4. Creating Empty DataFrame

# CELL ********************

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
schema = StructType([StructField("id",IntegerType(),True),
                     StructField("Name",StringType(),True)])
df = spark.createDataFrame([], schema)
df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 5. Creating DataFrame from Structured Data (CSV, JSON, Parquet)

# CELL ********************

# reading csv file into DataFrame
path = "Files/Pyspark_files/people.csv"
df_csv = spark.read.csv(path,header=True,sep=";")
display(df_csv)

# Reading JSON file
path = "Files/Pyspark_files/people.json"
df_json = spark.read.json(path)
display(df_json)

# Reading Multiline JSON file
path = "Files/Pyspark_files/multiline-zipcode.json"
df_multi_json = spark.read.json(path,multiLine=True)
display(df_multi_json)

# Reading Parquet Files
path = "Files/Pyspark_files/users.parquet"
df_parquet = spark.read.parquet(path)
display(df_parquet)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

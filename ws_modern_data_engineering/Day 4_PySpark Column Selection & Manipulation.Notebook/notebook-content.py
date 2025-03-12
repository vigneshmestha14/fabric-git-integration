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

# # PySpark Column Selection & Manipulation: Key Techniques
# ## 1. Different Methods to Select Columns
# ###### In PySpark, you can select specific columns in multiple ways:
# 
# ###### Using col() function / column() / string way:

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/Pyspark_files/BigMartSales.csv")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col,column

# Using col() function
df.select(col("Item_Identifier")).show(5)

# Using column() function
df.select(column("Item_Type")).show(5)

# Directly using string name
df.select("Outlet_Type").show(5)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 2. Selecting Multiple Columns Together
# ###### You can combine different methods to select multiple columns:

# CELL ********************

#multiple column
df_multiple_col = df.select("Item_Identifier", "Item_Fat_Content",
                col("Item_Weight"), column("Item_Visibility"),df.Item_Type )
df_multiple_col.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 3. Listing All Columns in a DataFrame
# ### To get a list of all the column names:

# CELL ********************

#get all column name
df.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 4.Renaming Columns with alias()
# ###### You can rename columns using the alias() method:

# CELL ********************

df.select(
    col("Item_Identifier").alias('Item_Id'), 
    col("Outlet_Identifier").alias('Outlet_Id'),
    "Item_MRP",
    column("Outlet_Type"),
    df.Item_Type
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

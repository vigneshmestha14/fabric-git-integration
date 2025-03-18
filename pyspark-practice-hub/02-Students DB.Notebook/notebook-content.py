# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1f4688c4-9112-42ba-8c5a-2c7045d78522",
# META       "default_lakehouse_name": "lh_sql_practice_hub",
# META       "default_lakehouse_workspace_id": "c6598bd0-bf59-4b02-a761-73171d8c2571"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # **Students DB**
# 
# ## **Problem Statement**
# Insert the following student details into the `students` table and print all data from the table.
# 
# ### **Student Data to Insert:**
# | ID  | Name   | Gender |
# |-----|--------|--------|
# | 3   | Kim    | F      |
# | 4   | Molina | F      |
# | 5   | Dev    | M      |
# 
# 
# ## **Notes**
# - Ensure the `students` table exists before running the query.
# - Verify that ID is a unique primary key.
# - Modify column names if needed to match the table schema.

# MARKDOWN ********************

# ## **Approach 1: Using PySpark DataFrame API**
# Instead of executing SQL, we use PySpark's DataFrame API to create and insert data.
# 
# ### **Steps**  
# 1. Create a `students` table if it does not exist.  
# 2. Insert the given student records into the table.  
# 3. Fetch and display all records.

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Initialize Spark session
spark = SparkSession.builder.appName("StudentDataInsert").getOrCreate()

# Define schema for the DataFrame
schema = StructType([
    StructField("ID", IntegerType(), False),
    StructField("Name", StringType(), False),
    StructField("Gender", StringType(), False)
])

# Create a DataFrame with student data
students_data = [
    (3, "Kim", "F"),
    (4, "Molina", "F"),
    (5, "Dev", "M")
]

students_df = spark.createDataFrame(students_data, schema)

# Show DataFrame contents
students_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Approach 2: SQL Query in PySpark**
# In this approach, we execute SQL queries directly within PySpark.
# 
# ### **Steps**  
# 1. Define `student` data as a DataFrame.   
# 2. Display all records.

# CELL ********************

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("StudentDataInsert").config("spark.sql.catalogImplementation","in-memory").getOrCreate()

# Create a temporary table (if not exists)
spark.sql("""
    CREATE TABLE IF NOT EXISTS students (
        ID INT,
        Name STRING,
        Gender STRING
    )
""")

# Insert data into the table
spark.sql("INSERT INTO students VALUES (3, 'Kim', 'F')")
spark.sql("INSERT INTO students VALUES (4, 'Molina', 'F')")
spark.sql("INSERT INTO students VALUES (5, 'Dev', 'M')")

# Retrieve and display all data
students_df = spark.sql("SELECT * FROM students")
students_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **Key Differences**
# | Approach | Uses SQL Queries | Uses DataFrame API | Suitable for Large Data |
# |----------|----------------|--------------------|----------------------|
# | **SQL in PySpark** | ✅ | ❌ | ✅ |
# | **DataFrame API** | ❌ | ✅ | ✅ |
# 
# Both approaches are valid; choose the one that best suits your use case. Let me know if you need modifications! 🚀

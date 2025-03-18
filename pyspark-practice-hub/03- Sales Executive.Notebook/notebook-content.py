# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # **Sales Executive**
# 
# ### **Problem Statement**
# Given three tables: **Salesperson**, **Company**, and **Orders**, output the names of all salespeople who **did not** sell to the company 'RED'.
# 
# ### **Example Data**
# 
# #### **Table: Salesperson**
# | sales_id | name  | salary  | commission_rate | hire_date  |
# |----------|-------|---------|-----------------|------------|
# | 1        | John  | 100000  | 6               | 4/1/2006   |
# | 2        | Amy   | 120000  | 5               | 5/1/2010   |
# | 3        | Mark  | 65000   | 12              | 12/25/2008 |
# | 4        | Pam   | 25000   | 25              | 1/1/2005   |
# | 5        | Alex  | 50000   | 10              | 2/3/2007   |
# 
# #### **Table: Company**
# | com_id | name   | city      |
# |--------|--------|-----------|
# | 1      | RED    | Boston    |
# | 2      | ORANGE | New York  |
# | 3      | YELLOW | Boston    |
# | 4      | GREEN  | Austin    |
# 
# #### **Table: Orders**
# | order_id | order_date | com_id | sales_id | amount  |
# |----------|------------|--------|----------|---------|
# | 1        | 1/1/2014   | 3      | 4        | 100000  |
# | 2        | 2/1/2014   | 4      | 5        | 5000    |
# | 3        | 3/1/2014   | 1      | 1        | 50000   |
# | 4        | 4/1/2014   | 1      | 4        | 25000   |
# 
# ### **Expected Output**
# | name  |
# |-------|
# | Amy   |
# | Mark  |
# | Alex  |
# 
# ---


# MARKDOWN ********************

# ## **🔥 Approach 1: Using PySpark DataFrame API**
# ### **Steps:**
# 1. **Initialize Spark Session**  
#    - Create a Spark session to work with DataFrames.
#    
# 2. **Load Data into DataFrames**  
#    - Read the `Salesperson`, `Company`, and `Orders` CSV files (assuming they are stored locally).  
#    
# 3. **Join `Orders` with `Company`**  
#    - Filter only orders related to the company `'RED'`.  
#    - Select the unique `sales_id` who sold to `'RED'`.  
# 
# 4. **Filter Salespersons Who Didn't Sell to `'RED'`**  
#    - Use **anti-join** to exclude salespeople who are in the previous filtered list.  
# 
# 5. **Display the result**  
#    - Select only the `name` column and display the output.  
# 
# 
# ---

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark Session
spark = SparkSession.builder.appName("SalesExecutive").getOrCreate()

# Create DataFrames for Salesperson, Company, and Orders
salesperson_data = [(1, "John", 100000, 6, "4/1/2006"),
                    (2, "Amy", 120000, 5, "5/1/2010"),
                    (3, "Mark", 65000, 12, "12/25/2008"),
                    (4, "Pam", 25000, 25, "1/1/2005"),
                    (5, "Alex", 50000, 10, "2/3/2007")]

salesperson_df = spark.createDataFrame(salesperson_data, ["sales_id", "name", "salary", "commission_rate", "hire_date"])

company_data = [(1, "RED", "Boston"),
                (2, "ORANGE", "New York"),
                (3, "YELLOW", "Boston"),
                (4, "GREEN", "Austin")]

company_df = spark.createDataFrame(company_data, ["com_id", "name", "city"])

orders_data = [(1, "1/1/2014", 3, 4, 100000),
               (2, "2/1/2014", 4, 5, 5000),
               (3, "3/1/2014", 1, 1, 50000),
               (4, "4/1/2014", 1, 4, 25000)]

orders_df = spark.createDataFrame(orders_data, ["order_id", "order_date", "com_id", "sales_id", "amount"])

# Find salespeople who sold to 'RED'
red_sales_df = orders_df.join(company_df, "com_id").filter(col("name") == "RED").select("sales_id")

# Find salespeople who didn't sell to 'RED'
result_df = salesperson_df.join(red_sales_df, "sales_id", "left_anti").select("name")

# Display result
result_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# 
# ## **🔥 Approach 2: Using SQL Query in PySpark**
# ### **Steps:**
# 1. **Initialize Spark Session**
# 2. **Load Data into DataFrames**
# 3. **Create Temporary Views**  
#    - Convert PySpark DataFrames into SQL temp tables.  
#    
# 4. **Write SQL Query to Solve the Problem**  
#    - Find all salespeople who **did** make sales to `'RED'`.  
#    - Use **NOT IN** to exclude them.  
# 
# 5. **Execute Query and Display Results**


# CELL ********************

# Step 1: Create Temporary Views
salesperson_df.createOrReplaceTempView("Salesperson")
company_df.createOrReplaceTempView("Company")
orders_df.createOrReplaceTempView("Orders")

# Step 2: Execute SQL Query
query = """
SELECT s.name
FROM Salesperson s
WHERE s.sales_id NOT IN (
    SELECT DISTINCT o.sales_id
    FROM Orders o
    JOIN Company c ON o.com_id = c.com_id
    WHERE c.name = 'RED'
)
"""

result_sql = spark.sql(query)

# Step 3: Display result
result_sql.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# ## **📝 Summary Table: Approach 1 vs Approach 2**
# | Approach | Method | Description |
# |----------|--------|-------------|
# | **Approach 1** | **DataFrame API** | Uses PySpark functions like `.join()`, `.filter()`, and `.left_anti` to find salespeople who never sold to `'RED'`. |
# | **Approach 2** | **SQL in PySpark** | Uses SQL `NOT IN` and `JOIN` to filter salespeople who never sold to `'RED'`. |
# 
# Both approaches yield the same result. 🚀

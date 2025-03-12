# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # **Broadcast Joins in PySpark**
# ###### Broadcast joins (also known as Map-side joins) are a type of optimization technique used when joining a large dataset with a small dataset. The smaller dataset is broadcast to all executor nodes, making the join operation more efficient.
# 
# ##### **When to Use Broadcast Joins:**
# ###### 1. One DataFrame is much smaller than the other
# ###### 2. The smaller DataFrame can fit in memory
# ###### 3. You want to avoid shuffle operations
# 
# ###### Here's an example of how to implement broadcast joins:

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

# Initialize Spark Session
spark = SparkSession.builder.appName("Broadcast Join Example").getOrCreate()

# Create sample DataFrames
# Large DataFrame
large_df = spark.createDataFrame([
    (1, "Product A"),
    (2, "Product B"),
    (3, "Product C"),
    (4, "Product D")
], ["product_id", "product_name"])

# Small DataFrame (good candidate for broadcasting)
small_df = spark.createDataFrame([
    (1, "Category 1"),
    (2, "Category 2")
], ["product_id", "category"])

# Method 1: Using broadcast hint
broadcast_join_df = large_df.join(broadcast(small_df), "product_id")
broadcast_join_df.explain()  # Shows the execution plan with broadcast
broadcast_join_df.show()

# Method 2: Using configuration
# Set broadcast threshold (default is 10MB)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10485760)  # 10MB in bytes

# Join will automatically use broadcast if small_df is below threshold
auto_broadcast_join_df = large_df.join(small_df, "product_id")
auto_broadcast_join_df.explain()
auto_broadcast_join_df.show()

# Disable broadcasting if needed
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Performance Comparison:

# CELL ********************

import time

# Large dataset simulation
large_data = [(i, f"Product_{i}") for i in range(100000)]
small_data = [(i, f"Category_{i%5}") for i in range(100)]

large_df = spark.createDataFrame(large_data, ["id", "product"])
small_df = spark.createDataFrame(small_data, ["id", "category"])

# Regular join
start_time = time.time()
regular_join = large_df.join(small_df, "id")
regular_join.count()
regular_time = time.time() - start_time

# Broadcast join
start_time = time.time()
broadcast_join = large_df.join(broadcast(small_df), "id")
broadcast_join.count()
broadcast_time = time.time() - start_time

print(f"Regular Join Time: {regular_time:.2f} seconds")
print(f"Broadcast Join Time: {broadcast_time:.2f} seconds")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Key broadcast configurations
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10485760)  # Set threshold to 10MB
spark.conf.set("spark.sql.shuffle.partitions", 200)  # Default shuffle partitions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## PySpark Broadcast Join Best Practices
# 
# ### Size Consideration
# - **Optimal Size**: Broadcast smaller DataFrame (< 10MB by default)
# - **Data Distribution**: Ensure even distribution across nodes

# CELL ********************

# Example of size checking before broadcast
def should_broadcast(df):
    # Get size estimate in bytes
    size_bytes = df._jdf.queryExecution().optimizedPlan().stats().sizeInBytes()
    size_mb = size_bytes / (1024 * 1024)
    return size_mb < 10  # Default threshold is 10MB

small_df = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "value"])
if should_broadcast(small_df):
    result = large_df.join(broadcast(small_df), "id")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Configuration Options:

# CELL ********************

# Key broadcast configurations
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10485760)  # Set threshold to 10MB
spark.conf.set("spark.sql.shuffle.partitions", 200)  # Default shuffle partitions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Effectiveness
# - **Smaller DataFrame**: Broadcast joins are most effective when one DataFrame is significantly smaller than the other.
# - **Avoid Expensive Shuffling**: Use broadcast joins to avoid expensive shuffling operations.
# - **Sufficient Memory**: Ensure you have enough memory on executor nodes to hold the broadcast DataFrame.

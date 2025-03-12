# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ## PySpark Shuffle Join Explanation
# 
# ### What is a Shuffle Join?
# A shuffle join in PySpark occurs when data needs to be redistributed across partitions before performing the join operation. This happens when the join key's data is not co-located on the same partition.
# 
# ### Implementation Examples
# #### Basic Shuffle Join

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Shuffle Join Example") \
    .getOrCreate()

# Create sample DataFrames
df1 = spark.createDataFrame([
    (1, "A", 1000),
    (2, "B", 2000),
    (3, "C", 3000)
], ["id", "name", "salary"])

df2 = spark.createDataFrame([
    (1, "HR"),
    (2, "IT"),
    (4, "Finance")
], ["id", "department"])

# Perform shuffle join
shuffle_join = df1.join(df2, "id")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Optimizing Shuffle Joins
# 1. Using Partitioning

# CELL ********************

# Repartition DataFrames before joining
df1_partitioned = df1.repartition(col("id"))
df2_partitioned = df2.repartition(col("id"))

optimized_join = df1_partitioned.join(df2_partitioned, "id")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 2. Controlling Shuffle Partitions

# CELL ********************

# Set number of shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", 10)

# Monitor partition size
def show_partition_counts(df, name):
    print(f"{name} partition count: {df.rdd.getNumPartitions()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Performance Monitoring

# CELL ********************

def analyze_join_performance(df1, df2, join_key):
    """Analyze join performance with different configurations"""
    
    # Original join
    start_time = time.time()
    regular_join = df1.join(df2, join_key)
    regular_time = time.time() - start_time
    
    # Optimized join with repartitioning
    start_time = time.time()
    optimized_join = df1.repartition(join_key).join(
        df2.repartition(join_key),
        join_key
    )
    optimized_time = time.time() - start_time
    
    print(f"Regular Join Time: {regular_time:.2f} seconds")
    print(f"Optimized Join Time: {optimized_time:.2f} seconds")
    print("\nExecution Plans:")
    print("Regular Join:")
    regular_join.explain()
    print("\nOptimized Join:")
    optimized_join.explain()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Best Practices
# 1. Partition Size Management


# CELL ********************

def optimize_partition_size(df, target_size_mb=128):
    """Optimize number of partitions based on data size"""
    total_bytes = df._jdf.queryExecution().optimizedPlan().stats().sizeInBytes()
    total_mb = total_bytes / (1024 * 1024)
    optimal_partitions = max(1, int(total_mb / target_size_mb))
    return df.repartition(optimal_partitions)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 2. Join Strategy Selection

# CELL ********************

def select_join_strategy(df1, df2, join_key):
    """Select appropriate join strategy based on DataFrame sizes"""
    df1_size = df1._jdf.queryExecution().optimizedPlan().stats().sizeInBytes()
    df2_size = df2._jdf.queryExecution().optimizedPlan().stats().sizeInBytes()
    
    broadcast_threshold = 10 * 1024 * 1024  # 10MB
    
    if min(df1_size, df2_size) < broadcast_threshold:
        # Use broadcast join for small DataFrames
        return df1.join(broadcast(df2), join_key)
    else:
        # Use shuffle join with optimized partitioning
        return df1.repartition(join_key).join(
            df2.repartition(join_key),
            join_key
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Monitoring and Debugging

# CELL ********************

def monitor_shuffle_metrics(df):
    """Monitor shuffle metrics for a DataFrame operation"""
    df.persist()  # Cache the DataFrame
    
    # Trigger computation and get metrics
    df.count()
    
    # Get Spark context
    sc = SparkSession.builder.getOrCreate().sparkContext
    
    # Print metrics
    print("Shuffle Metrics:")
    print(f"Shuffle Read: {sc.statusTracker().getExecutorMetrics()}")
    
    df.unpersist()  # Clean up

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Remember:
# 
# 1. Monitor shuffle spill metrics
# 2. Use appropriate number of partitions
# 3. Consider data skew
# 4. Test with representative data volumes

# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # Window Functions in PySpark - Part 3

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Creating a Spark Session

# CELL ********************

spark = SparkSession.builder.appName('WindowFunctionsPart3').getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Creating a Sample DataFrame

# CELL ********************

data = [
    ('Alice', 'Math', 90, 1), ('Alice', 'Science', 85, 1), ('Alice', 'History', 78, 1),
    ('Bob', 'Math', 80, 1), ('Bob', 'Science', 81, 1), ('Bob', 'History', 77, 1),
    ('Charlie', 'Math', 75, 1), ('Charlie', 'Science', 82, 1), ('Charlie', 'History', 79, 1),
    ('Alice', 'Physics', 86, 2), ('Alice', 'Chemistry', 92, 2), ('Alice', 'Biology', 80, 2),
    ('Bob', 'Physics', 94, 2), ('Bob', 'Chemistry', 91, 2), ('Bob', 'Biology', 96, 2),
    ('Charlie', 'Physics', 89, 2), ('Charlie', 'Chemistry', 88, 2), ('Charlie', 'Biology', 85, 2),
    ('Alice', 'Computer Science', 95, 3), ('Alice', 'Electronics', 91, 3), ('Alice', 'Geography', 97, 3),
    ('Bob', 'Computer Science', 88, 3), ('Bob', 'Electronics', 66, 3), ('Bob', 'Geography', 92, 3),
    ('Charlie', 'Computer Science', 92, 3), ('Charlie', 'Electronics', 97, 3), ('Charlie', 'Geography', 99, 3)
]

columns = ['First Name', 'Subject', 'Marks', 'Semester']
df = spark.createDataFrame(data, columns)
df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. Finding the Student Who Scored Maximum Marks in Each Semester

# CELL ********************

window_spec_max_marks = Window.partitionBy('Semester').orderBy(F.desc('Marks'))
max_marks_df = df.withColumn('Rank', F.rank().over(window_spec_max_marks))
top_scorer = max_marks_df.filter(max_marks_df['Rank'] == 1)
top_scorer.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Calculating the Percentage of Each Student Considering All Subjects

# CELL ********************

window_spec_total_marks = Window.partitionBy('First Name', 'Semester')
df = df.withColumn('TotalMarks', F.sum('Marks').over(window_spec_total_marks))
df = df.withColumn('Percentage', (F.col('TotalMarks') / (3 * 100)).cast('decimal(5, 2)')*100)
df2 = df.groupBy('First Name', 'Semester').agg(F.max('TotalMarks').alias('TotalMarks'),
                                             F.max('Percentage').alias('Percentage'))
df2.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Finding the Top Rank Holder in Each Semester

# CELL ********************

window_spec_rank = Window.partitionBy('Semester').orderBy(F.desc('Percentage'))
rank_df = df.withColumn('Rank', F.rank().over(window_spec_rank))
top_rank_holder = rank_df.filter(rank_df['Rank'] == 1).select('First Name','Semester', 'Rank', 'Percentage').distinct()
top_rank_holder.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Finding the Student Who Scored Maximum Marks in Each Subject in Each Semester

# CELL ********************

window_spec_max_subject_marks = Window.partitionBy('Semester', 'Subject').orderBy(F.desc('Marks'))
max_subject_marks_df = df.withColumn('Rank', F.rank().over(window_spec_max_subject_marks))
max_subject_scorer = max_subject_marks_df.filter(max_subject_marks_df['Rank'] == 1)
max_subject_scorer.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

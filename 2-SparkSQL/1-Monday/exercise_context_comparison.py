"""
Exercise: Context Comparison
============================
Week 2, Monday

Explore the relationship between SparkSession and SparkContext.
Complete the TODOs and answer the conceptual questions in comments.
"""

from pyspark.sql import SparkSession

# =============================================================================
# TASK 1: Understanding the Relationship
# =============================================================================

print("=== Task 1: SparkSession and SparkContext Relationship ===")

# TODO 1a: Create a SparkSession
spark = (
    SparkSession.builder
    .appName("ContextComparison")
    .master("local[*]")
    .getOrCreate()
)

# TODO 1b: Access the SparkContext
sc = spark.sparkContext  # SparkContext comes from the SparkSession

# TODO 1c: Prove they are connected
# Print app name from BOTH SparkSession and SparkContext
print(f"SparkSession app name: {spark.sparkContext.appName}")
print(f"SparkContext app name: {sc.appName}")

# Verify they share the same application ID
print(f"SparkSession app ID: {spark.sparkContext.applicationId}")
print(f"SparkContext app ID: {sc.applicationId}")

# TODO 1d: Answer these questions in comments below:
# Q1: Can you create a SparkContext after SparkSession exists?
# ANSWER:
# Usually, no. In normal PySpark usage you should NOT create a new SparkContext
# if one already exists (SparkSession already created/owns it).
#
# Q2: What happens if you try? (You can test this if you want)
# ANSWER:
# You typically get an error like:
# "ValueError: Cannot run multiple SparkContexts at once"
# or Spark will warn that an existing SparkContext is already running.
# Spark expects a single SparkContext per JVM/process.

# =============================================================================
# TASK 2: RDD vs DataFrame Operations
# =============================================================================

print("\n=== Task 2: RDD vs DataFrame Operations ===")

# TODO 2a: Create an RDD with [1, 2, 3, 4, 5]
rdd = sc.parallelize([1, 2, 3, 4, 5])

# TODO 2b: Create a DataFrame with the same data
# HINT: spark.createDataFrame([(1,), (2,), (3,), (4,), (5,)], ["value"])
df = spark.createDataFrame([(1,), (2,), (3,), (4,), (5,)], ["value"])

# TODO 2c: Double the values in the RDD using map()
rdd_doubled = rdd.map(lambda x: x * 2)

# TODO 2d: Double the values in the DataFrame using withColumn
from pyspark.sql.functions import col
df_doubled = df.withColumn("value_doubled", col("value") * 2)

# Print results
print("RDD doubled:")
print(rdd_doubled.collect())

print("DataFrame doubled:")
df_doubled.show()

# TODO 2e: Convert RDD to DataFrame
# simplest: turn ints into tuples, then create DF
rdd_to_df = rdd.map(lambda x: (x,)).toDF(["value"])
print("RDD -> DataFrame:")
rdd_to_df.show()

# TODO 2f: Convert DataFrame to RDD
df_to_rdd = df.rdd

print("DataFrame -> RDD (first 3):")
print(df_to_rdd.take(3))

# TODO 2g: Answer these questions:
# Q3: Which approach (RDD or DataFrame) felt more natural for this task?
# ANSWER:
# RDD felt more natural for simple math (map), but DataFrame is usually preferred
# in real projects because it has optimizations (Catalyst + Tungsten) and better
# built-in functions.
#
# Q4: What data type are the elements in df.rdd? (print first element to check)
# ANSWER:
# They are Row objects (pyspark.sql.types.Row). Example: Row(value=1)

# =============================================================================
# TASK 3: Broadcast and Accumulator Access
# =============================================================================

print("\n=== Task 3: Broadcast and Accumulator ===")

# TODO 3a: Create a broadcast variable with a lookup dictionary
# Example: {"NY": "New York", "CA": "California", "TX": "Texas"}
lookup_data = {"NY": "New York", "CA": "California", "TX": "Texas"}
broadcast_lookup = sc.broadcast(lookup_data)

# TODO 3b: Create an accumulator initialized to 0
counter = sc.accumulator(0)

# TODO 3c: Use both in an RDD operation
# Create an RDD of state codes and:
# 1. Map each code to its full name using the broadcast variable
# 2. Count how many items are processed using the accumulator

states_rdd = sc.parallelize(["NY", "CA", "TX", "NY", "CA"])

def map_state(code):
    counter.add(1)
    return broadcast_lookup.value.get(code, "UNKNOWN")

result = states_rdd.map(map_state)

# Trigger action so accumulator updates
print(f"Mapped states: {result.collect()}")
print(f"Items processed: {counter.value}")

# Q5: Why are broadcast and accumulator accessed via SparkContext instead of SparkSession?
# ANSWER:
# Because SparkContext is the low-level entry point to the Spark cluster/executors
# and manages distributed shared variables. SparkSession is a higher-level wrapper
# mainly for DataFrame/SQL APIs, but it still uses the SparkContext underneath.

# =============================================================================
# CONCEPTUAL QUESTIONS
# =============================================================================

print("\n=== Conceptual Questions ===")

# Answer these questions in the comments below:

# Q6: In a new PySpark 3.x project, which entry point would you use and why?
# ANSWER:
# SparkSession. It’s the modern unified entry point that covers DataFrame/SQL
# and gives access to SparkContext when you need RDD features.

# Q7: You inherit legacy Spark 1.x code that uses SQLContext.
#     What is the minimal change to modernize it?
# ANSWER:
# Replace SQLContext(sc) with SparkSession.builder.getOrCreate(),
# then use spark instead of sqlContext (e.g., spark.read / spark.sql).
# Minimal practical change:
#   from pyspark.sql import SparkSession
#   spark = SparkSession.builder.getOrCreate()

# Q8: Describe the relationship between SparkSession, SparkContext,
#     SQLContext, and HiveContext (you can use ASCII art):
# ANSWER:
#            SparkSession (spark)
#                 |
#                 +--> SparkContext (spark.sparkContext)  [RDDs, cluster connection]
#                 |
#                 +--> SQL features (replaces SQLContext)
#                 |
#                 +--> Hive support (replaces HiveContext if enableHiveSupport())
#
# In older Spark:
#   SparkContext --> SQLContext
#   SparkContext --> HiveContext (extends SQLContext)
# In Spark 2.x/3.x:
#   SparkSession unifies SQLContext + HiveContext + entry point for DataFrames

# =============================================================================
# CLEANUP
# =============================================================================

# TODO: Stop the SparkSession
spark.stop()
"""
Exercise: SparkSession Setup and Configuration
===============================================
Week 2, Monday
"""

from pyspark.sql import SparkSession

# =============================================================================
# TASK 1: Basic SparkSession Creation
# =============================================================================

# TODO 1a
spark = (
    SparkSession.builder
    .appName("MyFirstSparkSQLApp")
    .master("local[*]")
    .getOrCreate()
)

# TODO 1b
print("=== Task 1: Basic SparkSession ===")
print(f"Spark version: {spark.version}")
print(f"Application ID: {spark.sparkContext.applicationId}")
print(f"Default parallelism: {spark.sparkContext.defaultParallelism}")

# TODO 1c
data = [
    (1, "Alice", 25),
    (2, "Bob", 30),
    (3, "Charlie", 35),
    (4, "Diana", 28),
    (5, "Eve", 40)
]

columns = ["id", "name", "age"]

df = spark.createDataFrame(data, columns)

df.show()


# =============================================================================
# TASK 2: Configuration Exploration
# =============================================================================

print("\n=== Task 2: Configuration ===")

# TODO 2a
print(f"Shuffle partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")

# TODO 2b
# SQL configs -> use spark.conf.get(...)
print(f"Adaptive execution enabled: {spark.conf.get('spark.sql.adaptive.enabled')}")

# Spark (SparkConf) configs -> use spark.sparkContext.getConf().get(...)
conf = spark.sparkContext.getConf()
print(f"Driver memory: {conf.get('spark.driver.memory', 'NOT SET')}")
print(f"Executor memory: {conf.get('spark.executor.memory', 'NOT SET')}")

# TODO 2c
spark.conf.set("spark.sql.shuffle.partitions", 50)
print(f"Updated shuffle partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")

# COMMENT:
# Yes, it works. spark.conf.set() allows certain Spark SQL configurations
# to be changed at runtime. The new value takes effect immediately.


# =============================================================================
# TASK 3: getOrCreate() Behavior
# =============================================================================

print("\n=== Task 3: getOrCreate() Behavior ===")

# TODO 3a
spark2 = (
    SparkSession.builder
    .appName("DifferentName")
    .getOrCreate()
)

# TODO 3b
print(f"spark app name: {spark.sparkContext.appName}")
print(f"spark2 app name: {spark2.sparkContext.appName}")

# TODO 3c
print(f"Are spark and spark2 the same object? {spark is spark2}")

# TODO 3d
# EXPLANATION:
# getOrCreate() checks whether a SparkSession already exists.
# If one exists, it returns the existing session instead of creating a new one.
# Spark only allows one active SparkContext per application,
# so the original session is reused and the app name does NOT change.


# =============================================================================
# TASK 4: Session Cleanup
# =============================================================================

print("\n=== Task 4: Cleanup ===")

# Check before stopping
print(f"Is stopped before stop()? {spark.sparkContext._jsc.sc().isStopped()}")

# TODO 4a
spark.stop()


# =============================================================================
# STRETCH GOALS (Optional)
# =============================================================================

def create_my_spark_session(app_name, shuffle_partitions=100):
    """
    Creates a SparkSession with custom defaults.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .getOrCreate()
    )


# Stretch 2: Enable Hive support (optional)
# spark_hive = (
#     SparkSession.builder
#     .appName("HiveSession")
#     .master("local[*]")
#     .enableHiveSupport()
#     .getOrCreate()
# )

# Stretch 3: List all configuration options
# for conf in spark.sparkContext.getConf().getAll():
#     print(conf)
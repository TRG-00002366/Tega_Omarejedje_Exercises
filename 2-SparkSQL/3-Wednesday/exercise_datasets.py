"""
Exercise: Datasets and Type Safety
==================================
Week 2, Wednesday

Explore the Dataset/DataFrame paradigm and type-aware patterns in PySpark.
"""

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, lit

# =============================================================================
# SETUP - Do not modify
# =============================================================================

spark = SparkSession.builder.appName("Exercise: Datasets").master("local[*]").getOrCreate()

print("=== Exercise: Datasets and Type Safety ===")

# =============================================================================
# TASK 1: Understanding Row Objects (15 mins)
# =============================================================================

print("\n--- Task 1: Row Objects ---")

# TODO 1a: Create 3 Row objects for employees
employee1 = Row(name="Alice", age=30, department="Engineering")
employee2 = Row(name="Bob", age=25, department="Marketing")
employee3 = Row(name="Charlie", age=35, department="Sales")

# TODO 1b: Create a DataFrame from these Row objects
employees_df = spark.createDataFrame([employee1, employee2, employee3])
employees_df.show()

# TODO 1c: Access data from the first row using:
first_row = employees_df.first()
print(f"Attribute access: {first_row.name}")
print(f"Index access: {first_row[0]}")
print(f"Key access: {first_row['name']}")

# TODO 1d: Convert a Row to a dictionary
print(f"Row as dict: {first_row.asDict()}")

# =============================================================================
# TASK 2: Explicit Schemas (20 mins)
# =============================================================================

print("\n--- Task 2: Explicit Schemas ---")

# TODO 2a: Define a schema for a Product type
product_schema = StructType([
    StructField("id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("price", DoubleType(), nullable=True),
    StructField("category", StringType(), nullable=True)
])

# TODO 2b: Create sample product data and create DataFrame with the schema
products_data = [
    (1, "Laptop", 999.99, "Electronics"),
    (2, "Coffee Maker", 49.99, "Home"),
    (3, "Running Shoes", None, "Sports")  # Note: null price
]

products_df = spark.createDataFrame(products_data, schema=product_schema)
products_df.show()

# TODO 2c: Verify the schema matches your definition
products_df.printSchema()

# TODO 2d: What happens if you try to create data that violates the schema?
# Try creating data with wrong types and observe the error
# (Comment out after testing to avoid breaking the script)

# Example violations (uncomment one at a time to test):
# bad_products_data = [
#     ("NOT_INT", "Bad Laptop", 999.99, "Electronics")  # id should be int
# ]
# spark.createDataFrame(bad_products_data, schema=product_schema).show()

# =============================================================================
# TASK 3: Schema Validation (20 mins)
# =============================================================================

print("\n--- Task 3: Schema Validation ---")

# Incoming data that needs validation
incoming_data = spark.createDataFrame([
    (1, "Alice", 30, "Engineering"),
    (2, None, 25, "Marketing"),      # Null name - invalid
    (3, "Charlie", -5, "Sales"),     # Negative age - invalid
    (4, "Diana", 150, "Engineering") # Age too high - invalid
], ["id", "name", "age", "department"])

print("Incoming data:")
incoming_data.show()

# TODO 3a: Create a validation function that checks:
# - name is not null
# - age is between 18 and 100
# Return two DataFrames: valid and invalid

def validate_employees(df):
    """
    Validates employee data.
    Returns: (valid_df, invalid_df)
    """
    condition = (col("name").isNotNull()) & (col("age").between(18, 100))
    valid = df.filter(condition)
    invalid = df.filter(~condition)
    return valid, invalid

# TODO 3b: Apply validation and show results
valid_df, invalid_df = validate_employees(incoming_data)
print("Valid:")
valid_df.show()
print("Invalid:")
invalid_df.show()

# =============================================================================
# TASK 4: Working with RDD for Typed Transformations (20 mins)
# =============================================================================

print("\n--- Task 4: RDD Typed Transformations ---")

# Sample DataFrame
people = spark.createDataFrame([
    ("Alice", 30),
    ("Bob", 25),
    ("Charlie", 35)
], ["name", "age"])

# TODO 4a: Convert DataFrame to RDD
people_rdd = people.rdd

# TODO 4b: Use map() to transform each row, adding a category
def add_category(row):
    category = "Young" if row["age"] < 30 else "Senior"
    return Row(name=row["name"], age=row["age"], category=category)

typed_rdd = people_rdd.map(add_category)

# TODO 4c: Convert the transformed RDD back to DataFrame
typed_df = spark.createDataFrame(typed_rdd)
typed_df.show()

# TODO 4d: Why might you use RDD transformations instead of DataFrame?
# Answer in a comment:
# You might use RDD transformations when you need custom Python logic that’s awkward in SQL/DataFrame ops,
# or when working with low-level transformations, complex parsing, or you specifically want Python Row/object control.
# (In most real jobs, DataFrames are preferred for performance/optimizer benefits.)

# =============================================================================
# TASK 5: Schema Evolution (15 mins)
# =============================================================================

print("\n--- Task 5: Schema Evolution ---")

# Version 1 data
v1_data = spark.createDataFrame([
    (1, "Widget A", 19.99),
    (2, "Widget B", 29.99)
], ["id", "name", "price"])

# Version 2 data (has additional column)
v2_data = spark.createDataFrame([
    (3, "Widget C", 39.99, "Electronics"),
    (4, "Widget D", 49.99, "Home")
], ["id", "name", "price", "category"])

print("V1 schema:")
v1_data.printSchema()
print("V2 schema:")
v2_data.printSchema()

# TODO 5a: Evolve V1 data to match V2 schema (add missing column)
v1_evolved = v1_data.withColumn("category", lit(None).cast("string"))

# TODO 5b: Combine V1 and V2 data using unionByName
combined = v1_evolved.unionByName(v2_data)
combined.show()
combined.printSchema()

# =============================================================================
# CONCEPTUAL QUESTIONS
# =============================================================================

print("\n--- Conceptual Questions ---")

# Q1: In Scala/Java, what is the difference between Dataset[Row] and Dataset[Person]?
# ANSWER:
# Dataset[Row] is untyped (generic rows, like a DataFrame). Dataset[Person] is typed:
# Spark uses an Encoder for Person, allowing compile-time type checks and typed column access.

# Q2: Why does Python not have true typed Datasets like Scala?
# ANSWER:
# Python is dynamically typed and does not have JVM encoders in the same way.
# PySpark operates through a Python-JVM boundary, so you don’t get compile-time typed Dataset benefits.

# Q3: What are the benefits of using explicit schemas in production?
# ANSWER:
# - Prevents incorrect type inference
# - Catches bad data early
# - Makes pipelines predictable and stable
# - Improves readability and data contracts between teams
# - Helps performance by avoiding repeated inference

# Q4: When would schema validation at the application level be important?
# ANSWER:
# - When ingesting data from external/unknown sources (APIs, logs, user uploads)
# - When data quality rules matter (age ranges, required fields, business constraints)
# - When you must separate valid vs invalid rows for auditing / retries / reporting

# =============================================================================
# CLEANUP
# =============================================================================

spark.stop()
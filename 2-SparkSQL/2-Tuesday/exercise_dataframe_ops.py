"""
Exercise: DataFrame Operations
==============================
Week 2, Tuesday

Practice DataFrame creation, inspection, and basic operations.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, upper

# =============================================================================
# SETUP - Do not modify
# =============================================================================

spark = SparkSession.builder.appName("Exercise: DataFrame Ops").master("local[*]").getOrCreate()

# Sample employee data
employees = [
    (1, "Alice", "Engineering", 75000, "2020-01-15"),
    (2, "Bob", "Marketing", 65000, "2019-06-01"),
    (3, "Charlie", "Engineering", 80000, "2021-03-20"),
    (4, "Diana", "Sales", 55000, "2018-11-10"),
    (5, "Eve", "Marketing", 70000, "2020-09-05"),
    (6, "Frank", "Engineering", 72000, "2022-01-10"),
    (7, "Grace", "Sales", 58000, "2021-07-15")
]

df = spark.createDataFrame(employees, ["id", "name", "department", "salary", "hire_date"])

print("=== Exercise: DataFrame Operations ===")
print("\nEmployee Data:")
df.show()

# =============================================================================
# TASK 1: Schema Inspection (10 mins)
# =============================================================================

print("\n--- Task 1: Schema Inspection ---")

# TODO 1a: Print the schema using printSchema()
df.printSchema()

# TODO 1b: Print just the column names
print(f"Columns: {df.columns}")

# TODO 1c: Print the data types as a list of tuples
print(f"Dtypes: {df.dtypes}")

# TODO 1d: Print the total row count and column count
print(f"Row count: {df.count()}")
print(f"Column count: {len(df.columns)}")

# =============================================================================
# TASK 2: Column Selection (15 mins)
# =============================================================================

print("\n--- Task 2: Column Selection ---")

# TODO 2a: Select only name and salary columns
df.select("name", "salary").show()

# TODO 2b: Select all columns EXCEPT id (dynamically, not hardcoding column names)
cols_except_id = [c for c in df.columns if c != "id"]
df.select(*cols_except_id).show()

# TODO 2c: Use selectExpr to create a new column "monthly_salary" = salary / 12
df.selectExpr("*", "salary / 12 as monthly_salary").show()

# =============================================================================
# TASK 3: Adding and Modifying Columns (20 mins)
# =============================================================================

print("\n--- Task 3: Adding and Modifying Columns ---")

# TODO 3a: Add a column "country" with value "USA" for all rows
df_with_country = df.withColumn("country", lit("USA"))
df_with_country.show()

# TODO 3b: Add a column "salary_tier" based on salary:
#    - "Entry" if salary < 60000
#    - "Mid" if salary >= 60000 and < 75000
#    - "Senior" if salary >= 75000
df_with_tier = df_with_country.withColumn(
    "salary_tier",
    when(col("salary") < 60000, "Entry")
    .when((col("salary") >= 60000) & (col("salary") < 75000), "Mid")
    .otherwise("Senior")
)
df_with_tier.show()

# TODO 3c: Add a column "name_upper" with uppercase version of name
df_with_upper = df_with_tier.withColumn("name_upper", upper(col("name")))
df_with_upper.show()

# TODO 3d: Modify salary to increase by 5% (replace the column)
df_salary_raised = df_with_upper.withColumn("salary", col("salary") * 1.05)
df_salary_raised.show()

# =============================================================================
# TASK 4: Filtering Rows (20 mins)
# =============================================================================

print("\n--- Task 4: Filtering Rows ---")

# TODO 4a: Filter employees with salary > 70000
df.filter(col("salary") > 70000).show()

# TODO 4b: Filter employees in Engineering department
df.filter(col("department") == "Engineering").show()

# TODO 4c: Filter employees in Engineering OR Marketing
df.filter((col("department") == "Engineering") | (col("department") == "Marketing")).show()

# TODO 4d: Filter employees hired after 2020-01-01 with salary > 60000
df.filter((col("hire_date") > "2020-01-01") & (col("salary") > 60000)).show()

# =============================================================================
# TASK 5: Sorting (10 mins)
# =============================================================================

print("\n--- Task 5: Sorting ---")

# TODO 5a: Sort by salary ascending
df.orderBy(col("salary").asc()).show()

# TODO 5b: Sort by department ascending, then salary descending
df.orderBy(col("department").asc(), col("salary").desc()).show()

# =============================================================================
# TASK 6: Combining Operations (15 mins)
# =============================================================================

print("\n--- Task 6: Complete Pipeline ---")

# TODO 6: Create a complete pipeline that:
# 1. Filters to employees hired after 2020-01-01
# 2. Adds a 10% bonus column
# 3. Selects only name, department, salary, and bonus
# 4. Sorts by bonus descending
# 5. Shows the result
result = (
    df.filter(col("hire_date") > "2020-01-01")
      .withColumn("bonus", col("salary") * 0.10)
      .select("name", "department", "salary", "bonus")
      .orderBy(col("bonus").desc())
)

result.show()

# =============================================================================
# CLEANUP
# =============================================================================

spark.stop()
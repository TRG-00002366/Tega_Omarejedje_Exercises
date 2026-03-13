"""
Exercise: Interview Practice
============================
Week 2, Friday

Practice solving PySpark interview problems under time constraints.

INSTRUCTIONS:
1. Set a timer for each problem (time limits given)
2. Solve without looking at hints first
3. Check hints only if stuck
4. Review your solution against the expected output
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, avg, count, row_number, rank, desc, trim, lower, max as spark_max
from pyspark.sql.window import Window

# =============================================================================
# SETUP
# =============================================================================

spark = SparkSession.builder.appName("Interview Practice").master("local[*]").getOrCreate()

# =============================================================================
# PROBLEM 1: Data Cleaning (15 minutes)
# Difficulty: Easy
# =============================================================================

print("="*60)
print("PROBLEM 1: Data Cleaning")
print("Time Limit: 15 minutes")
print("="*60)

"""
You receive messy employee data. Clean it according to the requirements.

Requirements:
1. Remove rows where name is null
2. Replace null salaries with the department average
3. Trim whitespace from names
4. Ensure all department names are lowercase
5. Filter to employees earning above the company average
"""

employees = spark.createDataFrame([
    (1, "  Alice  ", "ENGINEERING", 75000),
    (2, "Bob", "engineering", 65000),
    (3, None, "Marketing", 70000),
    (4, " Charlie ", "MARKETING", None),
    (5, "Diana", "Sales", 55000),
    (6, "Eve", "sales", 80000)
], ["id", "name", "department", "salary"])

print("Input:")
employees.show()

# YOUR SOLUTION HERE:
employees_clean_base = (
    employees
    .filter(col("name").isNotNull())
    .withColumn("name", trim(col("name")))
    .withColumn("department", lower(col("department")))
)

dept_avg = employees_clean_base.groupBy("department").agg(
    avg("salary").alias("dept_avg_salary")
)

employees_with_avg = employees_clean_base.join(dept_avg, on="department", how="left")

employees_filled = employees_with_avg.withColumn(
    "salary",
    col("salary").cast("double")
).withColumn(
    "salary",
    col("salary")
).fillna({"dept_avg_salary": 0.0})

employees_filled = employees_filled.withColumn(
    "salary",
    col("salary")
)

from pyspark.sql.functions import when

employees_filled = employees_filled.withColumn(
    "salary",
    when(col("salary").isNull(), col("dept_avg_salary")).otherwise(col("salary"))
).drop("dept_avg_salary")

company_avg = employees_filled.agg(avg("salary").alias("company_avg")).first()["company_avg"]

cleaned_employees = employees_filled.filter(col("salary") > company_avg)

print("Cleaned employees:")
cleaned_employees.show()

# Expected:
# - Charlie should have Marketing's average salary
# - Only employees above company average shown
# - Names trimmed, departments lowercase


# =============================================================================
# PROBLEM 2: Window Functions (20 minutes)
# Difficulty: Medium
# =============================================================================

print("\n" + "="*60)
print("PROBLEM 2: Rank Top Products")
print("Time Limit: 20 minutes")
print("="*60)

"""
For each category, find the top 2 products by total sales.
Include the rank and show percentage of category total.

Expected columns: category, product, total_sales, rank, pct_of_category
"""

sales = spark.createDataFrame([
    ("Electronics", "Laptop", 5000),
    ("Electronics", "Phone", 3000),
    ("Electronics", "Tablet", 2000),
    ("Clothing", "Jacket", 1500),
    ("Clothing", "Shoes", 2500),
    ("Clothing", "Hat", 500),
    ("Home", "Lamp", 800),
    ("Home", "Rug", 1200)
], ["category", "product", "sales"])

print("Input:")
sales.show()

# YOUR SOLUTION HERE:
product_totals = sales.groupBy("category", "product").agg(
    spark_sum("sales").alias("total_sales")
)

category_totals = sales.groupBy("category").agg(
    spark_sum("sales").alias("category_total")
)

window_spec = Window.partitionBy("category").orderBy(desc("total_sales"))

top_products = (
    product_totals
    .join(category_totals, on="category", how="inner")
    .withColumn("rank", row_number().over(window_spec))
    .withColumn("pct_of_category", col("total_sales") / col("category_total") * 100)
    .filter(col("rank") <= 2)
    .select("category", "product", "total_sales", "rank", "pct_of_category")
)

print("Top 2 products by category:")
top_products.show()

# HINTS (only look if stuck!):
# - Use Window.partitionBy("category")
# - Use row_number() or rank()
# - For percentage: divide by category sum


# =============================================================================
# PROBLEM 3: Complex Join (20 minutes)
# Difficulty: Medium
# =============================================================================

print("\n" + "="*60)
print("PROBLEM 3: Customer Order Analysis")
print("Time Limit: 20 minutes")
print("="*60)

"""
Find customers who have NEVER placed an order.
Then find customers whose average order value is above $150.
"""

customers = spark.createDataFrame([
    (1, "Alice", "NY"),
    (2, "Bob", "CA"),
    (3, "Charlie", "TX"),
    (4, "Diana", "FL")
], ["customer_id", "name", "state"])

orders = spark.createDataFrame([
    (101, 1, 200),
    (102, 1, 150),
    (103, 2, 100),
    (104, 2, 80)
], ["order_id", "customer_id", "amount"])

print("Customers:")
customers.show()
print("Orders:")
orders.show()

# YOUR SOLUTION PART A - Customers with NO orders:
no_orders = customers.join(orders, on="customer_id", how="left_anti")
print("Customers with no orders:")
no_orders.show()

# YOUR SOLUTION PART B - Customers with avg order > $150:
high_value = (
    customers.join(orders, on="customer_id", how="inner")
    .groupBy("customer_id", "name", "state")
    .agg(avg("amount").alias("avg_order_value"))
    .filter(col("avg_order_value") > 150)
)
print("Customers with average order value > 150:")
high_value.show()


# =============================================================================
# PROBLEM 4: Optimization Question (15 minutes)
# Difficulty: Medium
# =============================================================================

print("\n" + "="*60)
print("PROBLEM 4: Optimize This Pipeline")
print("Time Limit: 15 minutes")
print("="*60)

"""
The following pipeline is inefficient. Identify the issues and rewrite it.
You do NOT need to run the code - just provide the optimized version.

Issues to look for:
- Missing caching
- Inefficient join patterns
- Partition issues
"""

# ORIGINAL (INEFFICIENT) CODE:
"""
df = spark.read.parquet("large_data.parquet")  # 100GB file

# Used 3 times
filtered = df.filter(col("amount") > 100)

# Report 1
report1 = filtered.groupBy("region").sum("amount")
report1.write.parquet("report1")

# Report 2  
report2 = filtered.groupBy("category").avg("amount")
report2.write.parquet("report2")

# Report 3
report3 = filtered.groupBy("date").count()
report3.write.parquet("report3")
"""

# YOUR OPTIMIZED VERSION (write in comments or code):
# What changes would you make and why?
#
# OPTIMIZED VERSION:
#
# df = spark.read.parquet("large_data.parquet")
#
# # Filter once and cache because reused 3 times
# filtered = df.filter(col("amount") > 100).cache()
# filtered.count()  # materialize cache
#
# # Optionally reduce shuffle partitions based on cluster size/data size
# spark.conf.set("spark.sql.shuffle.partitions", "reasonable_number")
#
# report1 = filtered.groupBy("region").agg(spark_sum("amount").alias("total_amount"))
# report1.coalesce(1).write.mode("overwrite").parquet("report1")
#
# report2 = filtered.groupBy("category").agg(avg("amount").alias("avg_amount"))
# report2.coalesce(1).write.mode("overwrite").parquet("report2")
#
# report3 = filtered.groupBy("date").agg(count("*").alias("row_count"))
# report3.coalesce(1).write.mode("overwrite").parquet("report3")
#
# filtered.unpersist()
#
# WHY:
# - cache() prevents recomputing the same filtered DataFrame 3 times
# - tuning shuffle partitions avoids excessive small shuffle tasks
# - coalesce() before write can reduce output file count


# =============================================================================
# PROBLEM 5: SQL Translation (15 minutes)
# Difficulty: Easy
# =============================================================================

print("\n" + "="*60)
print("PROBLEM 5: SQL Translation")
print("Time Limit: 15 minutes")
print("="*60)

"""
Convert this SQL query to DataFrame API:

SELECT 
    department,
    AVG(salary) as avg_salary,
    MAX(salary) as max_salary,
    COUNT(*) as emp_count
FROM employees
WHERE salary > 50000
GROUP BY department
HAVING COUNT(*) > 1
ORDER BY avg_salary DESC
"""

emp_data = spark.createDataFrame([
    ("Engineering", 80000),
    ("Engineering", 75000),
    ("Engineering", 70000),
    ("Marketing", 60000),
    ("Marketing", 55000),
    ("Sales", 45000),
    ("HR", 52000)
], ["department", "salary"])

print("Input:")
emp_data.show()

# YOUR SOLUTION:
result = (
    emp_data
    .filter(col("salary") > 50000)
    .groupBy("department")
    .agg(
        avg("salary").alias("avg_salary"),
        spark_max("salary").alias("max_salary"),
        count("*").alias("emp_count")
    )
    .filter(col("emp_count") > 1)
    .orderBy(col("avg_salary").desc())
)

print("SQL translated to DataFrame API:")
result.show()


# =============================================================================
# PROBLEM 6: Conceptual Questions (10 minutes)
# Difficulty: Easy
# =============================================================================

print("\n" + "="*60)
print("PROBLEM 6: Conceptual Questions")
print("Time Limit: 10 minutes (2 min each)")
print("="*60)

"""
Answer these questions concisely (1-2 sentences each):

Q1: What is the difference between transformation and action in Spark?
YOUR ANSWER:
Transformation creates a new DataFrame/RDD lazily without executing immediately.
Action triggers execution and returns a result or writes output.

Q2: When would you use repartition() vs coalesce()?
YOUR ANSWER:
Use repartition() when you need to increase partitions or reshuffle data evenly.
Use coalesce() when reducing partitions with less shuffle, usually before writes.

Q3: What causes a shuffle in Spark? Name two operations.
YOUR ANSWER:
A shuffle happens when Spark has to move data across partitions.
Examples include groupBy/groupByKey and join/orderBy/repartition.

Q4: Why is caching beneficial when a DataFrame is used multiple times?
YOUR ANSWER:
Caching avoids recomputing the same lineage repeatedly, which reduces runtime and cluster work.

Q5: What is the difference between cache() and persist()?
YOUR ANSWER:
cache() uses the default storage level.
persist() lets you choose a specific storage level, such as memory only or memory and disk.
"""


# =============================================================================
# SELF-ASSESSMENT
# =============================================================================

print("\n" + "="*60)
print("SELF-ASSESSMENT")
print("="*60)

"""
Rate yourself on each problem:
1 = Could not solve
2 = Solved with hints  
3 = Solved independently
4 = Solved quickly and optimally

Problem 1 (Cleaning):    2
Problem 2 (Windows):     2
Problem 3 (Joins):       2
Problem 4 (Optimization): 2
Problem 5 (SQL):         2
Problem 6 (Concepts):    2

TOTAL: 12/24

Areas to review:
- If scored 1-2 on cleaning: Review DataFrame operations
- If scored 1-2 on windows: Review window functions
- If scored 1-2 on joins: Review join types
- If scored 1-2 on optimization: Review caching and partitioning
"""


# =============================================================================
# CLEANUP
# =============================================================================

spark.stop()
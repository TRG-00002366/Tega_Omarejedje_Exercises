"""
Exercise: Aggregations
======================
Week 2, Tuesday

Practice groupBy and aggregate functions on sales data.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, avg, count, min, max, countDistinct

# =============================================================================
# SETUP - Do not modify
# =============================================================================

spark = SparkSession.builder.appName("Exercise: Aggregations").master("local[*]").getOrCreate()

# Sample sales data
sales = [
    ("2023-01", "Electronics", "Laptop", 1200, "Alice"),
    ("2023-01", "Electronics", "Phone", 800, "Bob"),
    ("2023-01", "Electronics", "Tablet", 500, "Alice"),
    ("2023-01", "Clothing", "Jacket", 150, "Charlie"),
    ("2023-01", "Clothing", "Shoes", 100, "Diana"),
    ("2023-02", "Electronics", "Laptop", 1300, "Eve"),
    ("2023-02", "Electronics", "Phone", 850, "Alice"),
    ("2023-02", "Clothing", "Jacket", 175, "Bob"),
    ("2023-02", "Clothing", "Pants", 80, "Charlie"),
    ("2023-03", "Electronics", "Laptop", 1100, "Frank"),
    ("2023-03", "Electronics", "Phone", 750, "Grace"),
    ("2023-03", "Clothing", "Shoes", 120, "Alice")
]

df = spark.createDataFrame(sales, ["month", "category", "product", "amount", "salesperson"])

print("=== Exercise: Aggregations ===")
print("\nSales Data:")
df.show()

# =============================================================================
# TASK 1: Simple Aggregations (15 mins)
# =============================================================================

print("\n--- Task 1: Simple Aggregations ---")

# TODO 1a: Calculate total, average, min, and max amount across ALL sales
summary_all = df.agg(
    spark_sum("amount").alias("total_amount"),
    avg("amount").alias("avg_amount"),
    min("amount").alias("min_amount"),
    max("amount").alias("max_amount")
)
summary_all.show()

# TODO 1b: Count the total number of sales transactions
total_txns = df.agg(count("*").alias("total_transactions"))
total_txns.show()

# TODO 1c: Count distinct categories
distinct_categories = df.agg(countDistinct("category").alias("distinct_categories"))
distinct_categories.show()

# =============================================================================
# TASK 2: GroupBy with Single Aggregation (15 mins)
# =============================================================================

print("\n--- Task 2: GroupBy Single Aggregation ---")

# TODO 2a: Total sales amount by category
df.groupBy("category").agg(
    spark_sum("amount").alias("total_sales")
).show()

# TODO 2b: Average sale amount by month
df.groupBy("month").agg(
    avg("amount").alias("avg_sale_amount")
).show()

# TODO 2c: Count of transactions by salesperson
df.groupBy("salesperson").agg(
    count("*").alias("transaction_count")
).show()

# =============================================================================
# TASK 3: GroupBy with Multiple Aggregations (20 mins)
# =============================================================================

print("\n--- Task 3: GroupBy Multiple Aggregations ---")

# TODO 3a: For each category, calculate:
# - Number of transactions
# - Total revenue
# - Average sale amount
# - Highest single sale
category_metrics = df.groupBy("category").agg(
    count("*").alias("num_transactions"),
    spark_sum("amount").alias("total_revenue"),
    avg("amount").alias("avg_sale_amount"),
    max("amount").alias("highest_sale")
)
category_metrics.show()

# TODO 3b: For each salesperson, calculate:
# - Number of sales
# - Total revenue
# - Distinct products sold (countDistinct)
salesperson_metrics = df.groupBy("salesperson").agg(
    count("*").alias("num_sales"),
    spark_sum("amount").alias("total_revenue"),
    countDistinct("product").alias("distinct_products_sold")
)
salesperson_metrics.show()

# =============================================================================
# TASK 4: Multi-Column GroupBy (15 mins)
# =============================================================================

print("\n--- Task 4: Multi-Column GroupBy ---")

# TODO 4a: Calculate total sales by month AND category
month_category_totals = df.groupBy("month", "category").agg(
    spark_sum("amount").alias("total_sales")
)
month_category_totals.show()

# TODO 4b: Find the top salesperson by month (hint: use multi-column groupBy)

# Step 1: revenue per (month, salesperson)
month_salesperson_totals = df.groupBy("month", "salesperson").agg(
    spark_sum("amount").alias("total_revenue")
)

# Step 2: max revenue per month
top_revenue_per_month = month_salesperson_totals.groupBy("month").agg(
    max("total_revenue").alias("max_revenue")
)

# Step 3: join back to get the salesperson(s) who match the max
m = top_revenue_per_month.alias("m")
t = month_salesperson_totals.alias("t")

top_salesperson_by_month = (
    m.join(
        t,
        (col("m.month") == col("t.month")) & (col("m.max_revenue") == col("t.total_revenue")),
        "inner"
    )
    .select(
        col("m.month").alias("month"),
        col("t.salesperson").alias("salesperson"),
        col("t.total_revenue").alias("total_revenue")
    )
)

top_salesperson_by_month.show()

# =============================================================================
# TASK 5: Filtering After Aggregation (15 mins)
# =============================================================================

print("\n--- Task 5: Filtering After Aggregation ---")

# TODO 5a: Find categories with total revenue > 2000
df.groupBy("category").agg(
    spark_sum("amount").alias("total_revenue")
).filter(
    col("total_revenue") > 2000
).show()

# TODO 5b: Find salespeople who made more than 2 transactions
df.groupBy("salesperson").agg(
    count("*").alias("transaction_count")
).filter(
    col("transaction_count") > 2
).show()

# TODO 5c: Find month-category combinations with average sale > 500
df.groupBy("month", "category").agg(
    avg("amount").alias("avg_sale_amount")
).filter(
    col("avg_sale_amount") > 500
).show()

# =============================================================================
# CHALLENGE: Business Questions (20 mins)
# =============================================================================

print("\n--- Challenge: Business Questions ---")

# TODO 6a: Which category had the highest average transaction value?
df.groupBy("category").agg(
    avg("amount").alias("avg_transaction_value")
).orderBy(
    col("avg_transaction_value").desc()
).show(1)

# TODO 6b: Who is the top salesperson by total revenue?
df.groupBy("salesperson").agg(
    spark_sum("amount").alias("total_revenue")
).orderBy(
    col("total_revenue").desc()
).show(1)

# TODO 6c: Which month had the most diverse products sold?
df.groupBy("month").agg(
    countDistinct("product").alias("distinct_products")
).orderBy(
    col("distinct_products").desc()
).show(1)

# =============================================================================
# CLEANUP
# =============================================================================

spark.stop()
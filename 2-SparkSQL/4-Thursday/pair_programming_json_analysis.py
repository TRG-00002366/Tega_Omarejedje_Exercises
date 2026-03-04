"""
Pair Programming: JSON Analysis Challenge
==========================================
Week 2, Thursday - Collaborative Exercise

ROLES: Switch Driver/Navigator every 20 minutes!

Load, flatten, and analyze complex nested JSON data.
Apply filtering, aggregations, and SQL queries.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, struct, sum as spark_sum, avg, countDistinct, row_number
from pyspark.sql.window import Window
import tempfile
import os

# =============================================================================
# SETUP
# =============================================================================

spark = SparkSession.builder \
    .appName("JSON Analysis") \
    .master("local[*]") \
    .getOrCreate()

# Create sample nested JSON data (simulating a real-world API response)
temp_dir = tempfile.mkdtemp()

# Complex nested JSON with orders, products, and customer details
json_data = """{"order_id": 1, "customer": {"id": 101, "name": "Alice", "address": {"city": "NYC", "zip": "10001"}}, "items": [{"product": "Laptop", "qty": 1, "price": 1200}, {"product": "Mouse", "qty": 2, "price": 25}], "date": "2023-01-15"}
{"order_id": 2, "customer": {"id": 102, "name": "Bob", "address": {"city": "LA", "zip": "90001"}}, "items": [{"product": "Phone", "qty": 1, "price": 800}], "date": "2023-01-16"}
{"order_id": 3, "customer": {"id": 101, "name": "Alice", "address": {"city": "NYC", "zip": "10001"}}, "items": [{"product": "Keyboard", "qty": 1, "price": 100}, {"product": "Monitor", "qty": 1, "price": 300}], "date": "2023-01-17"}
{"order_id": 4, "customer": {"id": 103, "name": "Charlie", "address": {"city": "Chicago", "zip": "60601"}}, "items": [{"product": "Tablet", "qty": 2, "price": 500}], "date": "2023-01-18"}"""

json_path = os.path.join(temp_dir, "orders.json")
with open(json_path, "w") as f:
    f.write(json_data)

print("=== Pair Programming: JSON Analysis ===")
print(f"\nLoading JSON from: {json_path}")

# =============================================================================
# PHASE 1: LOAD AND EXPLORE
# =============================================================================

print("\n" + "="*60)
print("PHASE 1: LOAD AND EXPLORE")
print("="*60)

# TODO 1a: Load the JSON file
orders = spark.read.json(json_path)

# TODO 1b: Print the schema to understand the structure
orders.printSchema()

# TODO 1c: Show the data
orders.show(truncate=False)

# TODO 1d: Answer these questions in comments:
# Q1: How many levels of nesting are there?
# ANSWER:
# - customer is a struct, and customer.address is another struct => 2 levels under customer
# - items is an array of structs => nested array + struct inside
# Overall: about 2-3 levels depending on how you count (root -> customer -> address, and root -> items -> element struct)
#
# Q2: What is the data type of the "items" field?
# ANSWER:
# Array of Structs (ArrayType of StructType)
#
# Q3: How would you access the customer's city?
# ANSWER:
# orders.select(col("customer.address.city")) or orders.select("customer.address.city")

# =============================================================================
# PHASE 2: FLATTEN NESTED STRUCTURES
# =============================================================================

print("\n" + "="*60)
print("PHASE 2: FLATTEN NESTED STRUCTURES")
print("="*60)

# TODO 2a: Extract customer information into flat columns
flat_orders = orders.select(
    col("order_id"),
    col("customer.id").alias("customer_id"),
    col("customer.name").alias("customer_name"),
    col("customer.address.city").alias("city"),
    col("customer.address.zip").alias("zip"),
    col("items"),
    col("date")
)

flat_orders.show(truncate=False)

# TODO 2b: Explode the items array into separate rows
exploded_orders = flat_orders.withColumn("item", explode(col("items")))

exploded_orders.show(truncate=False)

# TODO 2c: Create a fully flat DataFrame
fully_flat = exploded_orders.select(
    col("order_id"),
    col("customer_id"),
    col("customer_name"),
    col("city"),
    col("zip"),
    col("date"),
    col("item.product").alias("product"),
    col("item.qty").alias("qty"),
    col("item.price").alias("price")
)

# TODO 2d: Add a calculated column: line_total = qty * price
fully_flat = fully_flat.withColumn("line_total", col("qty") * col("price"))

fully_flat.show(truncate=False)

# =============================================================================
# PHASE 3: AGGREGATIONS
# =============================================================================

print("\n" + "="*60)
print("PHASE 3: AGGREGATIONS")
print("="*60)

# TODO 3a: Calculate order totals (sum of line_totals per order)
order_totals = fully_flat.groupBy("order_id").agg(
    spark_sum("line_total").alias("order_total")
).orderBy("order_id")

order_totals.show()

# TODO 3b: Find the most popular products (by total quantity sold)
popular_products = fully_flat.groupBy("product").agg(
    spark_sum("qty").alias("total_qty")
).orderBy(col("total_qty").desc())

popular_products.show()

# TODO 3c: Find total spending per customer
spend_per_customer = fully_flat.groupBy("customer_id", "customer_name").agg(
    spark_sum("line_total").alias("total_spent")
).orderBy(col("total_spent").desc())

spend_per_customer.show()

# TODO 3d: Find the city with the highest total order value
city_revenue = fully_flat.groupBy("city").agg(
    spark_sum("line_total").alias("total_city_revenue")
).orderBy(col("total_city_revenue").desc())

city_revenue.show()

# =============================================================================
# PHASE 4: SQL QUERIES
# =============================================================================

print("\n" + "="*60)
print("PHASE 4: SQL QUERIES")
print("="*60)

# TODO 4a: Register the flat DataFrame as a temp view
fully_flat.createOrReplaceTempView("orders_flat")

# TODO 4b: SQL query: customers who spent more than $1000 total
q_customers_over_1000 = """
SELECT customer_id, customer_name, SUM(line_total) AS total_spent
FROM orders_flat
GROUP BY customer_id, customer_name
HAVING SUM(line_total) > 1000
ORDER BY total_spent DESC
"""
spark.sql(q_customers_over_1000).show()

# TODO 4c: SQL query with a window function to rank products by revenue
q_rank_products = """
WITH product_revenue AS (
  SELECT product, SUM(line_total) AS revenue
  FROM orders_flat
  GROUP BY product
)
SELECT product, revenue,
       ROW_NUMBER() OVER (ORDER BY revenue DESC) AS revenue_rank
FROM product_revenue
ORDER BY revenue_rank
"""
spark.sql(q_rank_products).show()

# TODO 4d: SQL query to find the average order value by city
q_avg_order_by_city = """
WITH order_city_totals AS (
  SELECT order_id, city, SUM(line_total) AS order_total
  FROM orders_flat
  GROUP BY order_id, city
)
SELECT city, AVG(order_total) AS avg_order_value
FROM order_city_totals
GROUP BY city
ORDER BY avg_order_value DESC
"""
spark.sql(q_avg_order_by_city).show()

# =============================================================================
# PHASE 5: ADVANCED TRANSFORMATIONS
# =============================================================================

print("\n" + "="*60)
print("PHASE 5: ADVANCED TRANSFORMATIONS")
print("="*60)

# TODO 5a: Convert the flat DataFrame back to a nested JSON structure
# Result should have: order_id, customer (nested), total_amount
customer_info = fully_flat.select(
    "order_id",
    struct(
        col("customer_id").alias("id"),
        col("customer_name").alias("name"),
        struct(col("city").alias("city"), col("zip").alias("zip")).alias("address")
    ).alias("customer")
).dropDuplicates(["order_id"])

nested_result = customer_info.join(order_totals, on="order_id", how="inner") \
    .withColumnRenamed("order_total", "total_amount") \
    .orderBy("order_id")

nested_result.show(truncate=False)

# TODO 5b: Write the result as a JSON file
nested_out = os.path.join(temp_dir, "nested_output_json")
nested_result.coalesce(1).write.mode("overwrite").json(nested_out)
print(f"Wrote nested JSON output to: {nested_out}")

# TODO 5c: Create a summary report DataFrame
# - Total orders
# - Total revenue
# - Unique customers
# - Top product by quantity
# - Average order value

total_orders = orders.count()
total_revenue = order_totals.agg(spark_sum("order_total").alias("total_revenue")).first()["total_revenue"]
unique_customers = orders.select(col("customer.id").alias("cid")).distinct().count()
avg_order_value = order_totals.agg(avg("order_total").alias("avg_order_value")).first()["avg_order_value"]

top_product_row = popular_products.first()
top_product = top_product_row["product"]
top_product_qty = top_product_row["total_qty"]

summary_report = spark.createDataFrame(
    [(total_orders, float(total_revenue), unique_customers, top_product, int(top_product_qty), float(avg_order_value))],
    ["total_orders", "total_revenue", "unique_customers", "top_product", "top_product_qty", "avg_order_value"]
)

summary_report.show(truncate=False)

# =============================================================================
# CHALLENGE: Error Handling
# =============================================================================

print("\n" + "="*60)
print("CHALLENGE: Error Handling")
print("="*60)

# Malformed JSON data
malformed_json = """{"order_id": 5, "customer": {"id": 104, "name": "Diana"}, "items": [{"product": "Book", "qty": 1, "price": 20}]}
{"order_id": "not_a_number", "customer": {"id": 105}, "items": []}
{"order_id": 6, "bad_field": true}"""

malformed_path = os.path.join(temp_dir, "malformed.json")
with open(malformed_path, "w") as f:
    f.write(malformed_json)

# TODO 6a: Read with PERMISSIVE mode and show results
permissive_df = spark.read.option("mode", "PERMISSIVE").json(malformed_path)
print("PERMISSIVE mode:")
permissive_df.show(truncate=False)

# TODO 6b: Read with DROPMALFORMED mode and show results
dropmalformed_df = spark.read.option("mode", "DROPMALFORMED").json(malformed_path)
print("DROPMALFORMED mode:")
dropmalformed_df.show(truncate=False)

# TODO 6c: Discuss: When would you use each mode?
# Answer in comments:
# PERMISSIVE: Use when you want to keep as much data as possible and handle bad records later
#             (e.g., logging/corrupt record column, data quality pipelines).
# DROPMALFORMED: Use when bad records should be discarded completely and you only want clean rows
#                (e.g., strict production ingestion where malformed rows are unacceptable).

# =============================================================================
# DELIVERABLES
# =============================================================================

"""
ANALYSIS REPORT
===============
Complete this section with your findings:

1. Data Structure:
   - Number of orders: 4
   - Number of unique customers: 3
   - Number of unique products: 6

2. Key Insights:
   - Top customer by spending: Alice (customer_id 101) with 1650
   - Most popular product: Tablet (qty 2) OR Mouse (qty 2)  [tie]
   - City with highest revenue: NYC (1650)

3. SQL Queries Written:
   - Customers spending > 1000
   - Rank products by revenue (window function)
   - Average order value by city

4. Challenges Encountered:
   - Handling nested fields (customer.address) and exploding items array.
   - Being careful about totals (line_total) before aggregations.

5. Partner Contributions:
   - Partner A: Loading/exploring JSON, initial flattening plan, aggregations.
   - Partner B: SQL queries, nested JSON reconstruction, error handling modes.
"""

# =============================================================================
# CLEANUP
# =============================================================================

import shutil
shutil.rmtree(temp_dir)

spark.stop()
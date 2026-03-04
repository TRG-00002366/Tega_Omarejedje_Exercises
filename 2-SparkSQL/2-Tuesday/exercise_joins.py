"""
Exercise: Joins
===============
Week 2, Tuesday

Practice all join types with customer and order data.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast, sum as spark_sum

# =============================================================================
# SETUP - Do not modify
# =============================================================================

spark = SparkSession.builder.appName("Exercise: Joins").master("local[*]").getOrCreate()

# Customers
customers = spark.createDataFrame([
    (1, "Alice", "alice@email.com", "NY"),
    (2, "Bob", "bob@email.com", "CA"),
    (3, "Charlie", "charlie@email.com", "TX"),
    (4, "Diana", "diana@email.com", "FL"),
    (5, "Eve", "eve@email.com", "WA")
], ["customer_id", "name", "email", "state"])

# Orders
orders = spark.createDataFrame([
    (101, 1, "2023-01-15", 150.00),
    (102, 2, "2023-01-16", 200.00),
    (103, 1, "2023-01-17", 75.00),
    (104, 3, "2023-01-18", 300.00),
    (105, 6, "2023-01-19", 125.00),  # customer_id 6 does not exist!
    (106, 2, "2023-01-20", 180.00)
], ["order_id", "customer_id", "order_date", "amount"])

# Products (for multi-table join)
products = spark.createDataFrame([
    (101, "Laptop"),
    (102, "Phone"),
    (103, "Mouse"),
    (104, "Keyboard"),
    (107, "Monitor")  # Not in any order!
], ["order_id", "product_name"])

print("=== Exercise: Joins ===")
print("\nCustomers:")
customers.show()
print("Orders:")
orders.show()
print("Products:")
products.show()

# =============================================================================
# TASK 1: Inner Join (15 mins)
# =============================================================================

print("\n--- Task 1: Inner Join ---")

# TODO 1a: Join customers and orders (only matching records)
# Show customer name, order_id, order_date, amount
inner_join = customers.join(orders, on="customer_id", how="inner") \
    .select("name", "order_id", "order_date", "amount")

inner_join.show()

# TODO 1b: How many orders have matching customers?
matching_orders_count = customers.join(orders, on="customer_id", how="inner").count()
total_orders_count = orders.count()
print(f"Orders with matching customers: {matching_orders_count}")
print(f"Total orders: {total_orders_count}")

# =============================================================================
# TASK 2: Left and Right Joins (20 mins)
# =============================================================================

print("\n--- Task 2: Left and Right Joins ---")

# TODO 2a: LEFT JOIN - All customers, with order info where available
# Who has NOT placed any orders?
left_join = customers.join(orders, on="customer_id", how="left")
left_join.show()

no_orders = left_join.filter(col("order_id").isNull()).select("customer_id", "name", "state")
print("Customers with NO orders:")
no_orders.show()

# TODO 2b: RIGHT JOIN - All orders, with customer info where available
# Which order has no matching customer?
right_join = customers.join(orders, on="customer_id", how="right")
right_join.show()

no_customer = right_join.filter(col("name").isNull()).select("order_id", "customer_id", "order_date", "amount")
print("Orders with NO matching customer:")
no_customer.show()

# TODO 2c: What is the difference between the two results?
# Answer in a comment:
#
# LEFT JOIN keeps ALL customers (even if they have no orders).
# RIGHT JOIN keeps ALL orders (even if they have no customer).
#

# =============================================================================
# TASK 3: Full Outer Join (10 mins)
# =============================================================================

print("\n--- Task 3: Full Outer Join ---")

# TODO 3a: Perform a FULL OUTER join between customers and orders
full_join = customers.join(orders, on="customer_id", how="full")
full_join.show()

# TODO 3b: Filter to show only rows where there is a mismatch
mismatches = full_join.filter(col("order_id").isNull() | col("name").isNull())
print("Mismatched rows (customer without order OR order without customer):")
mismatches.show()

# =============================================================================
# TASK 4: Semi and Anti Joins (15 mins)
# =============================================================================

print("\n--- Task 4: Semi and Anti Joins ---")

# TODO 4a: LEFT SEMI JOIN - Customers who HAVE placed orders
customers_with_orders = customers.join(orders, on="customer_id", how="left_semi")
customers_with_orders.show()

# TODO 4b: LEFT ANTI JOIN - Customers who have NOT placed orders
customers_without_orders = customers.join(orders, on="customer_id", how="left_anti")
customers_without_orders.show()

# TODO 4c: When would you use anti join in real data work?
# Answer in a comment:
#
# Anti join is useful to find records in one dataset that do NOT have a match in another.
# Example: customers with no transactions, orphan records, missing dimension keys, data quality checks.
#

# =============================================================================
# TASK 5: Handling Duplicate Columns (15 mins)
# =============================================================================

print("\n--- Task 5: Handling Duplicate Columns ---")

# NOTE: Here we avoid ambiguity by joining on an explicit condition.
# After joining this way, you will have TWO customer_id columns:
# c.customer_id and o.customer_id

# TODO 5a: Join and then DROP the duplicate customer_id column
c = customers.alias("c")
o = orders.alias("o")

joined_dupes = c.join(o, col("c.customer_id") == col("o.customer_id"), "inner")

# drop the duplicate from orders side
dropped = joined_dupes.drop(col("o.customer_id"))
dropped.select(
    col("c.customer_id").alias("customer_id"),
    col("c.name").alias("name"),
    col("o.order_id").alias("order_id"),
    col("o.amount").alias("amount")
).show()

# TODO 5b: Alternative: Use aliases to reference specific columns
aliased_select = joined_dupes.select(
    col("c.customer_id").alias("customer_id"),
    col("c.name").alias("customer_name"),
    col("o.order_id").alias("order_id"),
    col("o.order_date").alias("order_date"),
    col("o.amount").alias("amount")
)
aliased_select.show()

# =============================================================================
# TASK 6: Multi-Table Join (15 mins)
# =============================================================================

print("\n--- Task 6: Multi-Table Join ---")

# TODO 6a: Join customers -> orders -> products
# Show: customer name, order_id, amount, product_name
c = customers.alias("c")
o = orders.alias("o")
p = products.alias("p")

# Keep all orders even if product missing? We'll decide in 6b.
multi_join = (
    c.join(o, col("c.customer_id") == col("o.customer_id"), "inner")
     .join(p, col("o.order_id") == col("p.order_id"), "left")
     .select(
        col("c.name").alias("customer_name"),
        col("o.order_id").alias("order_id"),
        col("o.amount").alias("amount"),
        col("p.product_name").alias("product_name")
     )
)

multi_join.show()

# TODO 6b: What kind of join should you use when some orders might not have products?
# ANSWER (comment):
# Use a LEFT join from orders to products so orders still appear even when product info is missing.

# =============================================================================
# CHALLENGE: Real-World Scenarios (20 mins)
# =============================================================================

print("\n--- Challenge: Real-World Scenarios ---")

# TODO 7a: Find the total spending per customer (only customers with orders)
spend_per_customer = (
    customers.join(orders, on="customer_id", how="inner")
    .groupBy("customer_id", "name")
    .agg(spark_sum("amount").alias("total_spent"))
    .orderBy(col("total_spent").desc())
)
spend_per_customer.show()

# TODO 7b: Find customers from CA who placed orders > $150
ca_big_orders = (
    customers.join(orders, on="customer_id", how="inner")
    .filter((col("state") == "CA") & (col("amount") > 150))
    .select("customer_id", "name", "state", "order_id", "amount")
)
ca_big_orders.show()

# TODO 7c: Find orders without valid product information (anti join pattern)
# orders that do NOT have matching order_id in products
orders_without_products = orders.join(products, on="order_id", how="left_anti")
orders_without_products.show()

# =============================================================================
# CLEANUP
# =============================================================================

spark.stop()
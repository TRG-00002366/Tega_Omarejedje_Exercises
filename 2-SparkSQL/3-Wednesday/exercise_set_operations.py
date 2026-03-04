"""
Exercise: Set Operations
========================
Week 2, Wednesday

Practice union, intersect, except operations on customer data.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

# =============================================================================
# SETUP - Do not modify
# =============================================================================

spark = SparkSession.builder.appName("Exercise: Set Ops").master("local[*]").getOrCreate()

# January customers
jan_customers = spark.createDataFrame([
    (1, "Alice", "alice@email.com"),
    (2, "Bob", "bob@email.com"),
    (3, "Charlie", "charlie@email.com"),
    (4, "Diana", "diana@email.com")
], ["customer_id", "name", "email"])

# February customers
feb_customers = spark.createDataFrame([
    (2, "Bob", "bob@email.com"),
    (4, "Diana", "diana@email.com"),
    (5, "Eve", "eve@email.com"),
    (6, "Frank", "frank@email.com")
], ["customer_id", "name", "email"])

# March customers (different column order!)
mar_customers = spark.createDataFrame([
    ("grace@email.com", "Grace", 7),
    ("henry@email.com", "Henry", 8),
    ("bob@email.com", "Bob", 2)  # Returning customer
], ["email", "name", "customer_id"])

print("=== Exercise: Set Operations ===")
print("\nJanuary Customers:")
jan_customers.show()
print("February Customers:")
feb_customers.show()
print("March Customers (different column order!):")
mar_customers.show()

# =============================================================================
# TASK 1: Union Operations (20 mins)
# =============================================================================

print("\n--- Task 1: Union ---")

# TODO 1a: Union January and February customers (keep duplicates)
jan_feb_union = jan_customers.union(feb_customers)
print("Union Jan + Feb (with duplicates):")
jan_feb_union.show()

# TODO 1b: Union January and February, then remove duplicates
jan_feb_unique = jan_feb_union.distinct()
print("Union Jan + Feb (distinct):")
jan_feb_unique.show()

# TODO 1c: Try union with March customers - what happens?
# Use unionByName to fix it
# NOTE: jan_customers.union(mar_customers) would fail because column order differs.
all_three = jan_customers.union(feb_customers).unionByName(mar_customers)
print("Union across all three months using unionByName:")
all_three.show()

# TODO 1d: How many unique customers do you have across all three months?
unique_all_three = all_three.dropDuplicates(["customer_id"])
print(f"Unique customers across Jan+Feb+Mar: {unique_all_three.count()}")
print("Unique customers across all three months:")
unique_all_three.orderBy("customer_id").show()

# =============================================================================
# TASK 2: Intersect (15 mins)
# =============================================================================

print("\n--- Task 2: Intersect ---")

# TODO 2a: Find customers who appear in BOTH January AND February
# Intersect requires same schema + same column order (Jan and Feb match).
jan_feb_intersect = jan_customers.intersect(feb_customers)
print("Customers in BOTH Jan and Feb:")
jan_feb_intersect.show()

# TODO 2b: Verify the result makes sense - who are the returning customers?
# Returning customers are Bob (2) and Diana (4).

# =============================================================================
# TASK 3: Subtract/Except (15 mins)
# =============================================================================

print("\n--- Task 3: Subtract/Except ---")

# TODO 3a: Find customers in January who did NOT return in February
jan_not_feb = jan_customers.exceptAll(feb_customers)
print("Customers in Jan but NOT in Feb:")
jan_not_feb.show()

# TODO 3b: Find NEW customers in February (not in January)
feb_not_jan = feb_customers.exceptAll(jan_customers)
print("New customers in Feb (not in Jan):")
feb_not_jan.show()

# TODO 3c: Business question: What is the customer churn from Jan to Feb?
# Answer in a comment:
# Churn from Jan to Feb = customers who were in Jan but not in Feb.
# That is: Alice (1) and Charlie (3). So churn count = 2 out of 4 January customers = 50%.

# =============================================================================
# TASK 4: Distinct and DropDuplicates (15 mins)
# =============================================================================

print("\n--- Task 4: Deduplication ---")

# Combined data with duplicates
all_data = jan_customers.union(feb_customers)

# TODO 4a: Use distinct() to remove exact duplicate rows
all_distinct = all_data.distinct()
print("Distinct rows (exact duplicates removed):")
all_distinct.show()

# TODO 4b: Use dropDuplicates() on email column only (Keep first occurrence)
email_dedup = all_data.dropDuplicates(["email"])
print("Drop duplicates by email only:")
email_dedup.show()

# TODO 4c: What is the difference between distinct() and dropDuplicates()?
# Answer in a comment:
# distinct() removes duplicate ROWS only when the entire row matches exactly.
# dropDuplicates(["email"]) removes duplicates based on a subset of columns (email here),
# keeping only the first row it encounters for each email.

# =============================================================================
# CHALLENGE: Data Reconciliation (20 mins)
# =============================================================================

print("\n--- Challenge: Data Reconciliation ---")

# System A data (source)
source = spark.createDataFrame([
    (1, "Product A", 100),
    (2, "Product B", 200),
    (3, "Product C", 300)
], ["id", "name", "price"])

# System B data (target)
target = spark.createDataFrame([
    (1, "Product A", 100),
    (2, "Product B", 250),  # Price difference!
    (4, "Product D", 400)   # New product!
], ["id", "name", "price"])

print("Source:")
source.show()
print("Target:")
target.show()

# TODO 5a: Find exact matches between source and target
matched = source.intersect(target)
print("Exact matches (source ∩ target):")
matched.show()

# TODO 5b: Find records in source but not in target (or different)
source_only = source.exceptAll(target)
print("Source-only (or different):")
source_only.show()

# TODO 5c: Find records in target but not in source (or different)
target_only = target.exceptAll(source)
print("Target-only (or different):")
target_only.show()

# TODO 5d: Create a reconciliation report showing:
# - Matched count
# - Source-only count
# - Target-only count
recon_report = spark.createDataFrame(
    [(matched.count(), source_only.count(), target_only.count())],
    ["matched_count", "source_only_count", "target_only_count"]
)
print("Reconciliation report:")
recon_report.show()

# =============================================================================
# CLEANUP
# =============================================================================

spark.stop()
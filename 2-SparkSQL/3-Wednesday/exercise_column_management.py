"""
Exercise: Column Management
===========================
Week 2, Wednesday

Practice adding, removing, and transforming columns on product inventory data.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, upper, lower, trim, concat, concat_ws,
    split, substring, regexp_replace, coalesce, current_date, initcap
)

# =============================================================================
# SETUP - Do not modify
# =============================================================================

spark = SparkSession.builder.appName("Exercise: Columns").master("local[*]").getOrCreate()

# Product inventory data (messy data for cleaning!)
inventory = spark.createDataFrame([
    (1, "  LAPTOP pro  ", "Electronics", 999.99, 50, None),
    (2, "  phone X ", "Electronics", 799.99, 100, "NY"),
    (3, "Winter JACKET", "Clothing", 149.99, 200, "CA"),
    (4, " running shoes ", "Clothing", 89.99, None, "TX"),
    (5, "coffee MAKER", "Home", 49.99, 75, None),
    (6, "  Desk Lamp  ", "Home", 29.99, 120, "NY")
], ["product_id", "product_name", "category", "price", "quantity", "warehouse"])

print("=== Exercise: Column Management ===")
print("\nRaw Inventory Data:")
inventory.show(truncate=False)

# =============================================================================
# TASK 1: String Cleaning (20 mins)
# =============================================================================

print("\n--- Task 1: String Cleaning ---")

# TODO 1a: Clean product_name: trim whitespace, convert to title case
inventory_clean_name = inventory.withColumn(
    "product_name",
    initcap(trim(col("product_name")))
)
inventory_clean_name.show(truncate=False)

# TODO 1b: Standardize category to lowercase
inventory_lower_cat = inventory_clean_name.withColumn(
    "category",
    lower(col("category"))
)
inventory_lower_cat.show(truncate=False)

# TODO 1c: Create a "product_code" column by:
# - Taking first 3 letters of category (uppercase)
# - Adding the product_id
# - Example: "ELE-1"
inventory_with_code = inventory_lower_cat.withColumn(
    "product_code",
    concat(upper(substring(col("category"), 1, 3)), lit("-"), col("product_id"))
)
inventory_with_code.show(truncate=False)

# =============================================================================
# TASK 2: Handling Nulls (15 mins)
# =============================================================================

print("\n--- Task 2: Handling Nulls ---")

# TODO 2a: Replace null warehouse with "CENTRAL"
inv_warehouse_filled = inventory_with_code.withColumn(
    "warehouse",
    coalesce(col("warehouse"), lit("CENTRAL"))
)

# TODO 2b: Replace null quantity with 0
inv_qty_filled = inv_warehouse_filled.withColumn(
    "quantity",
    coalesce(col("quantity"), lit(0))
)

inv_qty_filled.show(truncate=False)

# TODO 2c: Create an "in_stock" boolean column (quantity > 0)
inv_in_stock = inv_qty_filled.withColumn(
    "in_stock",
    col("quantity") > 0
)
inv_in_stock.show(truncate=False)

# =============================================================================
# TASK 3: Calculated Columns (20 mins)
# =============================================================================

print("\n--- Task 3: Calculated Columns ---")

# TODO 3a: Add "inventory_value" = price * quantity (handle nulls!)
inv_with_value = inv_in_stock.withColumn(
    "inventory_value",
    col("price") * coalesce(col("quantity"), lit(0))
)
inv_with_value.show(truncate=False)

# TODO 3b: Add "price_tier" based on price
inv_with_tier = inv_with_value.withColumn(
    "price_tier",
    when(col("price") < 50, "Budget")
    .when((col("price") >= 50) & (col("price") < 200), "Mid")
    .otherwise("Premium")
)
inv_with_tier.show(truncate=False)

# TODO 3c: Add "last_updated" column with today's date
inv_with_date = inv_with_tier.withColumn(
    "last_updated",
    current_date()
)
inv_with_date.show(truncate=False)

# =============================================================================
# TASK 4: Removing and Renaming (10 mins)
# =============================================================================

print("\n--- Task 4: Removing and Renaming ---")

# TODO 4a: Drop the "warehouse" column
inv_dropped = inv_with_date.drop("warehouse")

# TODO 4b: Rename columns:
# - product_id -> id
# - product_name -> name
inv_renamed = inv_dropped.withColumnRenamed("product_id", "id") \
                         .withColumnRenamed("product_name", "name")

inv_renamed.show(truncate=False)

# =============================================================================
# TASK 5: Complete Data Pipeline (25 mins)
# =============================================================================

print("\n--- Task 5: Complete Data Pipeline ---")

clean_inventory = (
    inventory
    # 1. Clean product_name (trim, title case)
    .withColumn("product_name", initcap(trim(col("product_name"))))
    # 2. Fill null warehouse with "CENTRAL"
    .withColumn("warehouse", coalesce(col("warehouse"), lit("CENTRAL")))
    # 3. Fill null quantity with 0
    .withColumn("quantity", coalesce(col("quantity"), lit(0)))
    # 4. Add inventory_value column
    .withColumn("inventory_value", col("price") * col("quantity"))
    # 5. Add price_tier column
    .withColumn(
        "price_tier",
        when(col("price") < 50, "Budget")
        .when((col("price") >= 50) & (col("price") < 200), "Mid")
        .otherwise("Premium")
    )
    # 6. Add last_updated column
    .withColumn("last_updated", current_date())
    # (also keep category standardized)
    .withColumn("category", lower(col("category")))
    # 7. Rename product_id to id, product_name to name
    .withColumnRenamed("product_id", "id")
    .withColumnRenamed("product_name", "name")
    # 8. Drop warehouse column
    .drop("warehouse")
    # 9. Order columns
    .select("id", "name", "category", "price", "quantity", "inventory_value", "price_tier", "last_updated")
)

clean_inventory.show(truncate=False)

# =============================================================================
# CHALLENGE: Extract and Parse (15 mins)
# =============================================================================

print("\n--- Challenge: String Parsing ---")

# Product descriptions
descriptions = spark.createDataFrame([
    ("Widget A - Size: Large, Color: Blue",),
    ("Gadget B - Size: Medium, Color: Red",),
    ("Tool C - Size: Small, Color: Green",)
], ["description"])

descriptions.show(truncate=False)

# TODO 6a: Extract just the product name (before the dash)
parsed = descriptions.withColumn(
    "product_name",
    trim(split(col("description"), "-").getItem(0))
)

# TODO 6b: Extract the size value
# Approach: remove everything up to "Size: " then split by comma
parsed = parsed.withColumn(
    "size",
    trim(split(regexp_replace(col("description"), ".*Size:\\s*", ""), ",").getItem(0))
)

# TODO 6c: Extract the color value
# Approach: remove everything up to "Color: "
parsed = parsed.withColumn(
    "color",
    trim(regexp_replace(col("description"), ".*Color:\\s*", ""))
)

parsed.show(truncate=False)

# =============================================================================
# CLEANUP
# =============================================================================

spark.stop()
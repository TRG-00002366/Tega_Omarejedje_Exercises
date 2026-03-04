"""
Pair Programming: Pipeline Optimization Challenge
=================================================
Week 2, Thursday - Collaborative Exercise

ROLES: Switch Driver/Navigator every 20 minutes!

This pipeline is SLOW. Your job is to optimize it using:
- Caching
- Partitioning
- Bucketing
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, avg, count
import time

# =============================================================================
# SETUP
# =============================================================================

spark = SparkSession.builder \
    .appName("Pipeline Optimization") \
    .master("local[*]") \
    .config("spark.sql.warehouse.dir", "spark-warehouse") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

# Generate sample data (simulating a larger dataset)
print("Generating sample data...")

# Sales transactions
sales_data = []
for i in range(50000):
    sales_data.append((
        i,
        i % 1000,           # customer_id
        i % 50,             # product_id
        100 + (i % 900),    # amount
        f"2023-{1 + (i % 12):02d}-{1 + (i % 28):02d}"  # date
    ))

sales = spark.createDataFrame(sales_data, ["txn_id", "customer_id", "product_id", "amount", "date"])

# Customers
customer_data = [(i, f"Customer_{i}", ["NY", "CA", "TX", "FL", "WA"][i % 5]) for i in range(1000)]
customers = spark.createDataFrame(customer_data, ["customer_id", "name", "state"])

# Products
product_data = [(i, f"Product_{i}", ["Electronics", "Clothing", "Home", "Sports"][i % 4]) for i in range(50)]
products = spark.createDataFrame(product_data, ["product_id", "product_name", "category"])

print(f"Sales: {sales.count()} rows")
print(f"Customers: {customers.count()} rows")
print(f"Products: {products.count()} rows")

# =============================================================================
# BASELINE PIPELINE (UNOPTIMIZED)
# =============================================================================

def run_baseline_pipeline():
    """The original, unoptimized pipeline."""
    print("\n" + "="*60)
    print("RUNNING BASELINE PIPELINE")
    print("="*60)

    start_time = time.time()

    # Report 1: Sales by Customer
    print("\nGenerating Report 1: Sales by Customer...")
    report1 = sales.join(customers, "customer_id") \
        .groupBy("customer_id", "name", "state") \
        .agg(spark_sum("amount").alias("total_spend"))
    report1.count()

    # Report 2: Sales by Product
    print("Generating Report 2: Sales by Product...")
    report2 = sales.join(products, "product_id") \
        .groupBy("product_id", "product_name", "category") \
        .agg(spark_sum("amount").alias("total_sales"))
    report2.count()

    # Report 3: Sales by State
    print("Generating Report 3: Sales by State...")
    report3 = sales.join(customers, "customer_id") \
        .groupBy("state") \
        .agg(
            spark_sum("amount").alias("total_sales"),
            count("*").alias("num_transactions")
        )
    report3.count()

    # Report 4: Top Customers per State
    print("Generating Report 4: Top Customers per State...")
    customer_totals = sales.join(customers, "customer_id") \
        .groupBy("customer_id", "name", "state") \
        .agg(spark_sum("amount").alias("total_spend"))

    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number, desc

    window = Window.partitionBy("state").orderBy(desc("total_spend"))
    report4 = customer_totals.withColumn("rank", row_number().over(window)) \
        .filter(col("rank") <= 5)
    report4.count()

    # Report 5: Monthly Trend
    print("Generating Report 5: Monthly Trend...")
    report5 = sales.join(products, "product_id") \
        .groupBy("category", "date") \
        .agg(spark_sum("amount").alias("daily_sales"))
    report5.count()

    end_time = time.time()
    baseline_time = end_time - start_time

    print(f"\nBASELINE COMPLETED in {baseline_time:.2f} seconds")
    return baseline_time


# =============================================================================
# PHASE 1: ANALYZE THE BASELINE
# =============================================================================

print("\n" + "="*60)
print("PHASE 1: ANALYZE THE BASELINE")
print("="*60)

baseline_time = run_baseline_pipeline()

# TODO 1b: Use .explain() to examine execution plans
print("\nEXPLAIN: sales join customers")
sales.join(customers, "customer_id").explain()

print("\nEXPLAIN: sales join products")
sales.join(products, "product_id").explain()

# TODO 1c: Identify issues (answers in comments)
# - How many times is sales.join(customers) computed?
#   ANSWER: 3 times (Report 1, Report 3, customer_totals for Report 4)
#
# - How many times is sales.join(products) computed?
#   ANSWER: 2 times (Report 2, Report 5)
#
# - What is the partition count?
#   ANSWER: By default, sales partitions depend on Spark. Shuffles use spark.sql.shuffle.partitions (200 here).
print(f"Sales partitions: {sales.rdd.getNumPartitions()}")
print(f"Customers partitions: {customers.rdd.getNumPartitions()}")
print(f"Products partitions: {products.rdd.getNumPartitions()}")
print(f"Shuffle partitions (current): {spark.conf.get('spark.sql.shuffle.partitions')}")

# =============================================================================
# PHASE 2: APPLY CACHING
# =============================================================================

print("\n" + "="*60)
print("PHASE 2: APPLY CACHING")
print("="*60)

def run_cached_pipeline():
    """Pipeline with caching optimization."""
    print("\nRUNNING CACHED PIPELINE")

    start_time = time.time()

    # Cache joins that are reused
    sales_customers = sales.join(customers, "customer_id").cache()
    sales_products = sales.join(products, "product_id").cache()

    # Materialize cache (IMPORTANT)
    sales_customers.count()
    sales_products.count()

    # Report 1: Sales by Customer
    report1 = sales_customers.groupBy("customer_id", "name", "state") \
        .agg(spark_sum("amount").alias("total_spend"))
    report1.count()

    # Report 2: Sales by Product
    report2 = sales_products.groupBy("product_id", "product_name", "category") \
        .agg(spark_sum("amount").alias("total_sales"))
    report2.count()

    # Report 3: Sales by State
    report3 = sales_customers.groupBy("state") \
        .agg(
            spark_sum("amount").alias("total_sales"),
            count("*").alias("num_transactions")
        )
    report3.count()

    # Report 4: Top Customers per State
    customer_totals = sales_customers.groupBy("customer_id", "name", "state") \
        .agg(spark_sum("amount").alias("total_spend"))

    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number, desc

    window = Window.partitionBy("state").orderBy(desc("total_spend"))
    report4 = customer_totals.withColumn("rank", row_number().over(window)) \
        .filter(col("rank") <= 5)
    report4.count()

    # Report 5: Monthly Trend
    report5 = sales_products.groupBy("category", "date") \
        .agg(spark_sum("amount").alias("daily_sales"))
    report5.count()

    # Unpersist (good practice)
    sales_customers.unpersist()
    sales_products.unpersist()

    end_time = time.time()
    cached_time = end_time - start_time
    print(f"CACHED COMPLETED in {cached_time:.2f} seconds")
    return cached_time

cached_time = run_cached_pipeline()
print(f"CACHED pipeline time: {cached_time:.2f}s")
print(f"Improvement: {(baseline_time - cached_time) / baseline_time * 100:.1f}%")

# =============================================================================
# PHASE 3: OPTIMIZE PARTITIONING
# =============================================================================

print("\n" + "="*60)
print("PHASE 3: OPTIMIZE PARTITIONING")
print("="*60)

def run_partitioned_pipeline():
    """Pipeline with partitioning optimization."""
    print("\nRUNNING PARTITIONED PIPELINE")
    start_time = time.time()

    # Reduce shuffle partitions for this data size
    spark.conf.set("spark.sql.shuffle.partitions", "8")

    # Repartition by join keys to improve locality before joins
    sales_by_customer = sales.repartition(8, "customer_id")
    sales_by_product = sales.repartition(8, "product_id")

    customers_by_customer = customers.repartition(8, "customer_id")
    products_by_product = products.repartition(8, "product_id")

    sales_customers = sales_by_customer.join(customers_by_customer, "customer_id").cache()
    sales_products = sales_by_product.join(products_by_product, "product_id").cache()

    sales_customers.count()
    sales_products.count()

    # Report 1
    sales_customers.groupBy("customer_id", "name", "state") \
        .agg(spark_sum("amount").alias("total_spend")) \
        .count()

    # Report 2
    sales_products.groupBy("product_id", "product_name", "category") \
        .agg(spark_sum("amount").alias("total_sales")) \
        .count()

    # Report 3
    sales_customers.groupBy("state") \
        .agg(spark_sum("amount").alias("total_sales"), count("*").alias("num_transactions")) \
        .count()

    # Report 4
    customer_totals = sales_customers.groupBy("customer_id", "name", "state") \
        .agg(spark_sum("amount").alias("total_spend"))

    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number, desc

    window = Window.partitionBy("state").orderBy(desc("total_spend"))
    customer_totals.withColumn("rank", row_number().over(window)) \
        .filter(col("rank") <= 5) \
        .count()

    # Report 5
    sales_products.groupBy("category", "date") \
        .agg(spark_sum("amount").alias("daily_sales")) \
        .count()

    sales_customers.unpersist()
    sales_products.unpersist()

    end_time = time.time()
    partitioned_time = end_time - start_time
    print(f"PARTITIONED COMPLETED in {partitioned_time:.2f} seconds")
    return partitioned_time

partitioned_time = run_partitioned_pipeline()
print(f"PARTITIONED pipeline time: {partitioned_time:.2f}s")
print(f"Improvement vs baseline: {(baseline_time - partitioned_time) / baseline_time * 100:.1f}%")

# =============================================================================
# PHASE 4: IMPLEMENT BUCKETING
# =============================================================================

print("\n" + "="*60)
print("PHASE 4: IMPLEMENT BUCKETING")
print("="*60)

def setup_bucketed_tables():
    """Create bucketed tables for optimized joins."""
    spark.sql("DROP TABLE IF EXISTS sales_bucketed")
    spark.sql("DROP TABLE IF EXISTS customers_bucketed")
    spark.sql("DROP TABLE IF EXISTS products_bucketed")

    # Important: bucketing works with saveAsTable
    sales.write.bucketBy(16, "customer_id").sortBy("customer_id") \
        .mode("overwrite").saveAsTable("sales_bucketed")

    customers.write.bucketBy(16, "customer_id").sortBy("customer_id") \
        .mode("overwrite").saveAsTable("customers_bucketed")

    products.write.bucketBy(16, "product_id").sortBy("product_id") \
        .mode("overwrite").saveAsTable("products_bucketed")

    print("Bucketed tables created: sales_bucketed, customers_bucketed, products_bucketed")

def run_bucketed_pipeline():
    """Pipeline using bucketed tables."""
    print("\nRUNNING BUCKETED PIPELINE")
    start_time = time.time()

    sales_b = spark.table("sales_bucketed")
    customers_b = spark.table("customers_bucketed")
    products_b = spark.table("products_bucketed")

    # Explain to verify join behavior
    print("\nEXPLAIN: bucketed sales join bucketed customers")
    sales_b.join(customers_b, "customer_id").explain()

    print("\nEXPLAIN: bucketed sales join bucketed products")
    sales_b.join(products_b, sales_b.product_id == products_b.product_id).explain()

    # For reports, reuse joins (cache)
    sales_customers = sales_b.join(customers_b, "customer_id").cache()
    sales_products = sales_b.join(products_b, sales_b.product_id == products_b.product_id) \
        .drop(products_b.product_id).cache()

    sales_customers.count()
    sales_products.count()

    sales_customers.groupBy("customer_id", "name", "state").agg(spark_sum("amount").alias("total_spend")).count()
    sales_products.groupBy("product_id", "product_name", "category").agg(spark_sum("amount").alias("total_sales")).count()
    sales_customers.groupBy("state").agg(spark_sum("amount").alias("total_sales"), count("*").alias("num_transactions")).count()

    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number, desc

    customer_totals = sales_customers.groupBy("customer_id", "name", "state").agg(spark_sum("amount").alias("total_spend"))
    window = Window.partitionBy("state").orderBy(desc("total_spend"))
    customer_totals.withColumn("rank", row_number().over(window)).filter(col("rank") <= 5).count()

    sales_products.groupBy("category", "date").agg(spark_sum("amount").alias("daily_sales")).count()

    sales_customers.unpersist()
    sales_products.unpersist()

    end_time = time.time()
    bucketed_time = end_time - start_time
    print(f"BUCKETED COMPLETED in {bucketed_time:.2f} seconds")
    return bucketed_time

setup_bucketed_tables()
bucketed_time = run_bucketed_pipeline()

# =============================================================================
# PHASE 5: FINAL OPTIMIZED PIPELINE
# =============================================================================

print("\n" + "="*60)
print("PHASE 5: FINAL OPTIMIZED PIPELINE")
print("="*60)

def run_optimized_pipeline():
    """The fully optimized pipeline with all techniques applied."""
    print("\nRUNNING FULLY OPTIMIZED PIPELINE")

    spark.conf.set("spark.sql.shuffle.partitions", "8")
    start_time = time.time()

    # Use bucketed tables (best-case join setup)
    sales_b = spark.table("sales_bucketed")
    customers_b = spark.table("customers_bucketed")
    products_b = spark.table("products_bucketed")

    # Cache reused joins
    sales_customers = sales_b.join(customers_b, "customer_id").cache()
    sales_products = sales_b.join(products_b, sales_b.product_id == products_b.product_id) \
        .drop(products_b.product_id).cache()

    sales_customers.count()
    sales_products.count()

    # Same reports
    sales_customers.groupBy("customer_id", "name", "state").agg(spark_sum("amount").alias("total_spend")).count()
    sales_products.groupBy("product_id", "product_name", "category").agg(spark_sum("amount").alias("total_sales")).count()
    sales_customers.groupBy("state").agg(spark_sum("amount").alias("total_sales"), count("*").alias("num_transactions")).count()

    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number, desc

    customer_totals = sales_customers.groupBy("customer_id", "name", "state").agg(spark_sum("amount").alias("total_spend"))
    window = Window.partitionBy("state").orderBy(desc("total_spend"))
    customer_totals.withColumn("rank", row_number().over(window)).filter(col("rank") <= 5).count()

    sales_products.groupBy("category", "date").agg(spark_sum("amount").alias("daily_sales")).count()

    sales_customers.unpersist()
    sales_products.unpersist()

    end_time = time.time()
    optimized_time = end_time - start_time
    print(f"OPTIMIZED COMPLETED in {optimized_time:.2f} seconds")
    return optimized_time

# =============================================================================
# PERFORMANCE COMPARISON
# =============================================================================

print("\n" + "="*60)
print("PERFORMANCE COMPARISON")
print("="*60)

optimized_time = run_optimized_pipeline()
improvement = (baseline_time - optimized_time) / baseline_time * 100

print(f"Baseline time:     {baseline_time:.2f}s")
print(f"Cached time:       {cached_time:.2f}s")
print(f"Partitioned time:  {partitioned_time:.2f}s")
print(f"Bucketed time:     {bucketed_time:.2f}s")
print(f"Optimized time:    {optimized_time:.2f}s")
print(f"Improvement:       {improvement:.1f}%")

# =============================================================================
# CLEANUP
# =============================================================================

spark.sql("DROP TABLE IF EXISTS sales_bucketed")
spark.sql("DROP TABLE IF EXISTS customers_bucketed")
spark.sql("DROP TABLE IF EXISTS products_bucketed")

spark.stop()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, sum as _sum

#Task 1: Create Your First PySpark Script
def main():
    # Step 1: Create SparkSession
    spark = SparkSession.builder \
        .appName("MyFirstJob") \
        .master("local[*]") \
        .getOrCreate()
    
# Task 2: Work with a Simple Dataset
    sales_data = [
        ("Laptop", "Electronics", 999.99, 5),
        ("Mouse", "Electronics", 29.99, 50),
        ("Desk", "Furniture", 199.99, 10),
        ("Chair", "Furniture", 149.99, 20),
        ("Monitor", "Electronics", 299.99, 15),
    ]
    
    # Step 3: YOUR CODE HERE - Perform transformations
    # Create DataFrame with column names
    df = spark.createDataFrame(sales_data, ["product", "category", "price", "quantity"])
    
    
# Step 3: Perform transformations
    # revenue = price * quantity (rounded to 2 decimals for nicer display)
    df_with_revenue = df.withColumn("revenue", round(col("price") * col("quantity"), 2))

    electronics_only = df.filter(col("category") == "Electronics")

    revenue_by_category = (
        df_with_revenue
        .groupBy("category")
        .agg(round(_sum("revenue"), 2).alias("total_revenue"))
        .orderBy("category")
    )

    # Step 4: Show results
    print("\nOriginal DataFrame:")
    df.show()

    total_products = df.count()
    print(f"Total products: {total_products}\n")

    print("Revenue per product:")
    df_with_revenue.show()

    print("Electronics only:")
    electronics_only.show()

    print("Revenue by category:")
    revenue_by_category.show()

   
    
    # Step 5: Clean up
    spark.stop()

if __name__ == "__main__":
    main()
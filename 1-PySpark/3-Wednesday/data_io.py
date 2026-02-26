from pyspark import SparkContext

sc = SparkContext("local[*]", "DataIO")

# -------------------------
# Task 1: Load Data with textFile
# -------------------------

# Load the CSV file
lines = sc.textFile("sales_data.csv")

# Skip header line
header = lines.first()
data = lines.filter(lambda line: line != header)

print(f"Header: {header}")
print(f"Data records: {data.count()}")
print(f"First record: {data.first()}")

# -------------------------
# Task 2: Parse CSV Records
# -------------------------

def parse_record(line):
    """Parse CSV line into structured data."""
    parts = line.split(",")
    return {
        "product_id": parts[0],
        "name": parts[1],
        "category": parts[2],
        "price": float(parts[3]),
        "quantity": int(parts[4])
    }

# Parse all records
parsed = data.map(parse_record)

# Show parsed data
for record in parsed.take(3):
    print(record)

# -------------------------
# Task 3: Process and Save Results
# -------------------------

# Calculate revenue for each product
revenue = parsed.map(lambda r: f"{r['product_id']},{r['name']},{r['price'] * r['quantity']:.2f}")

# Save to output directory
# YOUR CODE: Use saveAsTextFile to save revenue data
revenue.saveAsTextFile("output_revenue")

print("Saved revenue output to: output_revenue/")

# -------------------------
# Task 4: Load Multiple Files
# -------------------------
# YOUR CODE: Create sales_data_2.csv with more records (do this manually as a file)
# Example contents:
# product_id,name,category,price,quantity
# P008,Webcam,Electronics,59.99,40
# P009,Table,Furniture,249.99,8

# YOUR CODE: Load all CSV files using wildcard pattern
all_lines = sc.textFile("sales_data*.csv")

all_header = all_lines.first()
all_data = all_lines.filter(lambda line: line != all_header)

print(f"All data records (wildcard): {all_data.count()}")
print(f"Sample wildcard record: {all_data.first()}")

# -------------------------
# Task 5: Coalesce Output
# -------------------------

# Re-parse wildcard data, compute revenue, and save as a single output part
all_parsed = all_data.map(parse_record)

all_revenue = all_parsed.map(lambda r: f"{r['product_id']},{r['name']},{r['price'] * r['quantity']:.2f}")

# YOUR CODE: Use coalesce(1) before saveAsTextFile
all_revenue.coalesce(1).saveAsTextFile("output_revenue_single")

print("Saved single-part revenue output to: output_revenue_single/")

sc.stop()
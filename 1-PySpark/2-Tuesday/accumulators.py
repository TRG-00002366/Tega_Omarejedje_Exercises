from pyspark import SparkContext

sc = SparkContext("local[*]", "AccumulatorExercise")

# -------------------------
# Task 1: Basic Counter Accumulator
# -------------------------

# Create accumulator
record_counter = sc.accumulator(0)

# Sample data
data = sc.parallelize(range(1, 101))

# Count records using accumulator
def count_record(x):
    record_counter.add(1)
    return x

data.map(count_record).collect()

print(f"Records processed: {record_counter.value}")
# Expected: 100

# -------------------------
# Task 2: Error Counting Pattern
# -------------------------

# Sample data with some invalid records
records = sc.parallelize([
    "100,Alice,Engineering",
    "200,Bob,Sales",
    "INVALID_RECORD",
    "300,Charlie,Marketing",
    "",  # Empty record
    "400,Diana,Engineering",
    "BAD_DATA_HERE",
    "500,Eve,Sales"
])

# Accumulators for tracking
total_records = sc.accumulator(0)
valid_records = sc.accumulator(0)
invalid_records = sc.accumulator(0)

def validate_record(record):
    total_records.add(1)

    # Check if record has 3 comma-separated fields
    if record is None or record.strip() == "":
        invalid_records.add(1)
        return None

    parts = record.split(",")

    if len(parts) == 3:
        valid_records.add(1)
        return record
    else:
        invalid_records.add(1)
        return None

valid_data = records.map(validate_record).filter(lambda x: x is not None)
valid_data.collect()

print(f"Total records: {total_records.value}")
print(f"Valid records: {valid_records.value}")
print(f"Invalid records: {invalid_records.value}")
print(f"Error rate: {invalid_records.value / total_records.value * 100:.1f}%")

# Expected:
# Total records: 8
# Valid records: 5
# Invalid records: 3
# Error rate: 37.5%

# -------------------------
# Task 3: Category Counter
# -------------------------

# Sample data
sales = sc.parallelize([
    ("Electronics", 999),
    ("Clothing", 50),
    ("Electronics", 299),
    ("Food", 25),
    ("Clothing", 75),
    ("Electronics", 149),
    ("Food", 30)
])

# Create accumulators for each category
electronics_count = sc.accumulator(0)
clothing_count = sc.accumulator(0)
food_count = sc.accumulator(0)

def count_by_category(record):
    category, _ = record
    # Increment the appropriate accumulator
    if category == "Electronics":
        electronics_count.add(1)
    elif category == "Clothing":
        clothing_count.add(1)
    elif category == "Food":
        food_count.add(1)
    return record

sales.foreach(count_by_category)

print(f"Electronics: {electronics_count.value}")
print(f"Clothing: {clothing_count.value}")
print(f"Food: {food_count.value}")

# Expected:
# Electronics: 3
# Clothing: 2
# Food: 2

# -------------------------
# Task 4: Sum Accumulator
# -------------------------

# Calculate total sales amount
total_sales = sc.accumulator(0)

def sum_sales(record):
    _, amount = record
    total_sales.add(amount)
    return record

sales.foreach(sum_sales)

print(f"Total sales: ${total_sales.value}")
# Expected: $1627

sc.stop()
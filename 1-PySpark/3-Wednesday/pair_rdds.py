from pyspark import SparkContext

sc = SparkContext("local[*]", "PairRDDs")

# -------------------------
# Task 1: Classic Word Count
# -------------------------

# Sample text
text = sc.parallelize([
    "Apache Spark is a fast and general engine",
    "Spark provides APIs in Python Java and Scala",
    "Spark is used for big data processing",
    "PySpark is the Python API for Spark"
])

# Implement Word Count:
# 1. Split lines into words
# 2. Convert to (word, 1) pairs
# 3. Sum counts by key
# 4. Sort by count descending

words = text.flatMap(lambda line: line.lower().split())
pairs = words.map(lambda w: (w, 1))
counts = pairs.reduceByKey(lambda a, b: a + b)
word_counts = counts.sortBy(lambda kv: kv[1], ascending=False)

print("Word Counts (top 10):")
for word, count in word_counts.take(10):
    print(f"  {word}: {count}")

# -------------------------
# Task 2: Join Operations
# -------------------------

# Products
products = sc.parallelize([
    ("P001", "Laptop"),
    ("P002", "Mouse"),
    ("P003", "Keyboard"),
    ("P004", "Monitor")
])

# Prices
prices = sc.parallelize([
    ("P001", 999),
    ("P002", 29),
    ("P003", 79),
    ("P005", 199)  # Note: P005 not in products
])

# Task A: Inner join
inner = products.join(prices)
print(f"Inner join: {inner.collect()}")

# Task B: Left outer join (keep all products)
left = products.leftOuterJoin(prices)
print(f"Left join: {left.collect()}")

# Task C: Right outer join (keep all prices)
right = products.rightOuterJoin(prices)
print(f"Right join: {right.collect()}")

# Task D: Full outer join
full = products.fullOuterJoin(prices)
print(f"Full join: {full.collect()}")

# -------------------------
# Task 4: Aggregation Operations
# -------------------------

# Employee data: (department, (name, salary))
employees = sc.parallelize([
    ("Engineering", ("Alice", 90000)),
    ("Engineering", ("Bob", 85000)),
    ("Sales", ("Charlie", 70000)),
    ("Engineering", ("Diana", 95000)),
    ("Sales", ("Eve", 75000)),
    ("HR", ("Frank", 60000))
])

# Task A: Count employees per department
dept_counts = employees.mapValues(lambda _: 1).reduceByKey(lambda a, b: a + b)
print(f"Employee counts: {dept_counts.collect()}")

# Task B: Sum salaries per department
dept_salaries = employees.mapValues(lambda x: x[1]).reduceByKey(lambda a, b: a + b)
print(f"Total salaries: {dept_salaries.collect()}")

# Task C: Average salary per department (hint: use aggregateByKey or combine count+sum)
# We'll compute (sum, count) then map to average
dept_sum_count = employees.mapValues(lambda x: (x[1], 1)).reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)
dept_avg_salary = dept_sum_count.mapValues(lambda scnt: scnt[0] / scnt[1])
print(f"Average salaries: {dept_avg_salary.collect()}")

# -------------------------
# Task 5: sortByKey
# -------------------------

# Sort word counts alphabetically
alphabetical = word_counts.sortByKey()
print(f"Alphabetical: {alphabetical.take(10)}")

# Sort by key descending
reverse = word_counts.sortByKey(ascending=False)
print(f"Reverse: {reverse.take(10)}")

sc.stop()
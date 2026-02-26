from pyspark import SparkContext

sc = SparkContext("local[*]", "Transformations")

# -------------------------
# Task 1: Narrow Transformations
# -------------------------

# Sample log data
logs = sc.parallelize([
    "2024-01-15 10:00:00 INFO User login: alice",
    "2024-01-15 10:01:00 ERROR Database connection failed",
    "2024-01-15 10:02:00 INFO User login: bob",
    "2024-01-15 10:03:00 WARN Memory usage high",
    "2024-01-15 10:04:00 ERROR Timeout occurred",
    "2024-01-15 10:05:00 INFO Data processed: 1000 records",
    "2024-01-15 10:06:00 DEBUG Cache hit rate: 95%"
])

# Task A: Filter only ERROR logs
errors = logs.filter(lambda line: " ERROR " in line)
print(f"Errors: {errors.collect()}")

# Task B: Extract just the log level from each line
# Expected: ["INFO", "ERROR", "INFO", "WARN", "ERROR", "INFO", "DEBUG"]
levels = logs.map(lambda line: line.split()[2])
print(f"Levels: {levels.collect()}")

# Task C: Chain - get messages from ERROR logs only
# Expected: ["Database connection failed", "Timeout occurred"]
error_messages = (
    logs
    .filter(lambda line: line.split()[2] == "ERROR")
    .map(lambda line: " ".join(line.split()[3:]))
)
print(f"Error messages: {error_messages.collect()}")

# -------------------------
# Task 2: Wide Transformations
# -------------------------

# Sample word data
words = sc.parallelize([
    "spark", "hadoop", "spark", "data", "hadoop",
    "spark", "python", "data", "spark", "scala"
])

# Task A: distinct() - Get unique words
unique = words.distinct()
print(f"Unique words: {unique.collect()}")

# Task B: Group and count (wide transformation)
word_counts = words.map(lambda w: (w, 1)).reduceByKey(lambda a, b: a + b)
print(f"Word counts: {word_counts.collect()}")

# Task C: sortBy - Sort by count descending
sorted_counts = word_counts.sortBy(lambda kv: kv[1], ascending=False)
print(f"Sorted: {sorted_counts.collect()}")

# -------------------------
# Task 3: Set Operations
# -------------------------

# Two datasets
set1 = sc.parallelize([1, 2, 3, 4, 5])
set2 = sc.parallelize([4, 5, 6, 7, 8])

# Task A: union() - Combine both sets
combined = set1.union(set2)
print(f"Union: {combined.collect()}")

# Task B: intersection() - Common elements
common = set1.intersection(set2)
print(f"Intersection: {common.collect()}")

# Task C: subtract() - Elements in set1 but not in set2
difference = set1.subtract(set2)
print(f"Difference: {difference.collect()}")

# -------------------------
# Task 4: Practical Pipeline
# -------------------------

# Given: Web server logs
web_logs = sc.parallelize([
    "192.168.1.1 GET /home 200 150ms",
    "192.168.1.2 GET /products 200 230ms",
    "192.168.1.1 POST /login 200 180ms",
    "192.168.1.3 GET /home 404 50ms",
    "192.168.1.2 GET /products 200 210ms",
    "192.168.1.1 GET /home 200 120ms",
    "192.168.1.4 GET /admin 403 30ms"
])

# Build a pipeline to:
# 1. Filter only successful requests (status 200)
# 2. Extract the endpoint (e.g., /home)
# 3. Count requests per endpoint
# 4. Sort by count descending
endpoint_counts = (
    web_logs
    .filter(lambda line: line.split()[3] == "200")
    .map(lambda line: (line.split()[2], 1))
    .reduceByKey(lambda a, b: a + b)
    .sortBy(lambda kv: kv[1], ascending=False)
)

print(f"Endpoint counts: {endpoint_counts.collect()}")
# Expected output: [('/products', 2), ('/home', 2), ('/login', 1)]

sc.stop()
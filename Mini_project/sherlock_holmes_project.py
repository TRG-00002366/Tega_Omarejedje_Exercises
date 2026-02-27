import os
import re
import string
from pyspark import SparkContext

sc = SparkContext("local[*]", "SherlockHolmesProject")

# -----------------------------
# O6: Load file 
# -----------------------------
path = "data/sherlock_holmes.txt"

lines = sc.textFile(path)

# -----------------------------
# O2: Accumulator (blank lines)
# -----------------------------
blank_lines = sc.accumulator(0)

def count_blank(line):
    if line.strip() == "":
        blank_lines.add(1)
    return line

# action to update accumulator
lines.map(count_blank).count()

# -----------------------------
# O1: Broadcast stopwords
# -----------------------------
stopwords_list = ["the", "a", "an", "is", "and", "or", "to", "of", "in", "that", "it", "for", "on", "as"]
stopwords = sc.broadcast(set(stopwords_list))

# -----------------------------
# T1: normalize (lowercase + remove punctuation)
# -----------------------------
punct_table = str.maketrans("", "", string.punctuation)

normalized = lines.map(lambda line: line.lower().translate(punct_table))

# -----------------------------
# T2: tokenize words
# -----------------------------
words = normalized.flatMap(lambda line: line.split())

# -----------------------------
# T3: remove stopwords (uses broadcast)
# -----------------------------
clean_words = words.filter(lambda w: w not in stopwords.value)

# -----------------------------
# T4: total character count (approx includes newline)
# -----------------------------
total_chars = int(lines.map(lambda line: len(line) + 1).sum())
print(f"\nT4 Total characters: {total_chars}")

# -----------------------------
# T5: longest line + length
# -----------------------------
longest = lines.map(lambda line: (len(line), line)).max()
print(f"\nT5 Longest line length: {longest[0]}")
print("T5 Longest line:")
print(longest[1])

# -----------------------------
# T6: lines containing 'Watson'
# -----------------------------
watson_lines = lines.filter(lambda line: "watson" in line.lower())
print(f"\nT6 Lines containing Watson: {watson_lines.count()}")
print("T6 Sample (first 5):")
for l in watson_lines.take(5):
    print(l)

# -----------------------------
# T7: unique vocabulary count
# -----------------------------
unique_vocab = words.distinct().count()
print(f"\nT7 Unique vocabulary count: {unique_vocab}")

# -----------------------------
# T8: top 10 frequent words (whole book)
# -----------------------------
word_counts = words.map(lambda w: (w, 1)).reduceByKey(lambda a, b: a + b)
top10 = word_counts.sortBy(lambda x: x[1], ascending=False).take(10)
print("\nT8 Top 10 frequent words:")
for w, c in top10:
    print(f"  {w}: {c}")

# -----------------------------
# T9: first word of each line + counts
# -----------------------------
first_words = (
    normalized
    .filter(lambda line: line.strip() != "")
    .map(lambda line: line.split()[0])
    .map(lambda w: (w, 1))
    .reduceByKey(lambda a, b: a + b)
)
print("\nT9 First-word counts (top 10):")
for w, c in first_words.sortBy(lambda x: x[1], ascending=False).take(10):
    print(f"  {w}: {c}")

# -----------------------------
# T10: average word length
# -----------------------------
total_word_len = words.map(lambda w: len(w)).sum()
total_word_count = words.count()
avg_len = total_word_len / total_word_count
print(f"\nT10 Average word length: {avg_len:.2f}")

# -----------------------------
# T11: distribution of word lengths
# -----------------------------
length_dist = words.map(lambda w: (len(w), 1)).reduceByKey(lambda a, b: a + b).sortByKey()
print("\nT11 Word length distribution (first 15 lengths):")
for length, cnt in length_dist.take(15):
    print(f"  {length}-letter words: {cnt}")

# -----------------------------
# T12: extract between headings 
# -----------------------------
start_heading = "I. A SCANDAL IN BOHEMIA"
end_heading = "II. THE RED-HEADED LEAGUE"

indexed = lines.zipWithIndex()  # (line, index)

start = indexed.filter(lambda x: start_heading in x[0]).take(1)
end = indexed.filter(lambda x: end_heading in x[0]).take(1)

if start and end:
    start_idx = start[0][1]
    end_idx = end[0][1]

    chapter = (
        indexed
        .filter(lambda x: x[1] >= start_idx and x[1] < end_idx)
        .map(lambda x: x[0])
    )

    print(f"\nT12 Extracted lines count: {chapter.count()}")
    print("T12 Preview first 20 lines:")
    for line in chapter.take(20):
        print(line)
else:
    print("Could not find chapter headings.")

# -----------------------------
# O3: word-length pairing (Pair RDD)
# -----------------------------
word_length_pairs = words.map(lambda w: (len(w), w))
print("\nO3 Word-length pairs (first 10):")
print(word_length_pairs.take(10))

# -----------------------------
# O4: character frequency (letters only)
# -----------------------------
letters = normalized.flatMap(lambda line: list(line)).filter(lambda ch: ch >= "a" and ch <= "z")
char_counts = letters.map(lambda ch: (ch, 1)).reduceByKey(lambda a, b: a + b)
print("\nO4 Letter frequency (top 10):")
for ch, c in char_counts.sortBy(lambda x: x[1], ascending=False).take(10):
    print(f"  {ch}: {c}")

# -----------------------------
# O5: group words by starting letter (groupByKey)
# -----------------------------
grouped = words.map(lambda w: (w[0], w)).groupByKey().mapValues(list)
print("\nO5 Grouped words sample (letters a, s, t):")
for letter, lst in grouped.filter(lambda x: x[0] in ["a", "s", "t"]).take(3):
    print(f"  {letter}: {lst[:10]} (showing first 10)")

# -----------------------------
# O7: save word count output
# -----------------------------
output_dir = "Holmes_WordCount_Results"
# Spark fails if output_dir already exists
try:
    word_counts.map(lambda x: f"{x[0]},{x[1]}").saveAsTextFile(output_dir)
    print(f"\nO7 Saved word counts to: {output_dir}/")
except Exception as e:
    print(f"\nO7 Could not save (folder may already exist): {e}")

print(f"\nO2 Blank lines found: {blank_lines.value}")

sc.stop()
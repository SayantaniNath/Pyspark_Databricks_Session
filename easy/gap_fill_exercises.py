# Gap-Fill Exercises — Core DataFrame API
# Topics: string functions, datetime, when/otherwise, null handling, set ops
# Instructions: write from blank — no peeking at theory
# Run on Databricks CE or local PySpark

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("gap_fill").getOrCreate()

# Sample data — transactions
data = [
    (1, "alice smith", "2024-01-15", 250.0, None),
    (2, "BOB JONES", "2024-02-20", 180.5, "premium"),
    (3, "carol white", "2024-03-05", None, "basic"),
    (4, "david brown", "2024-01-28", 320.0, "premium"),
    (5, "EVE DAVIS", "2024-04-10", 95.0, None),
]
columns = ["id", "name", "txn_date", "amount", "tier"]

df = spark.createDataFrame(data, columns)
df = df.withColumn("txn_date", F.to_date("txn_date"))

# ─────────────────────────────────────────
# STRING FUNCTIONS (Easy 11)
# ─────────────────────────────────────────

# Ex 1: Standardize name to title case (capitalize first letter of each word)
# Expected: "Alice Smith", "Bob Jones", etc.
# Hint: F.initcap()


# Ex 2: Split name into first_name and last_name columns
# Expected: first_name="alice", last_name="smith"
# Hint: F.split(), then index with [0] and [1]


# Ex 3: Add a column email = first_name + "." + last_name + "@company.com" (lowercase)
# Expected: "alice.smith@company.com"
# Hint: F.concat(), F.lower(), F.split()


# Ex 4: Add a column name_length = total character count of the name column
# Hint: F.length()


# ─────────────────────────────────────────
# DATETIME FUNCTIONS (Easy 12)
# ─────────────────────────────────────────

# Ex 5: Extract year, month, day as separate columns from txn_date
# Hint: F.year(), F.month(), F.dayofmonth()


# Ex 6: Add a column days_since_txn = number of days between txn_date and today (2026-06-07)
# Hint: F.datediff(F.lit("2026-06-07"), F.col("txn_date"))


# Ex 7: Format txn_date as "DD-MMM-YYYY" string (e.g. "15-Jan-2024")
# Hint: F.date_format(col, "dd-MMM-yyyy")


# ─────────────────────────────────────────
# NULL HANDLING + WHEN/OTHERWISE (Easy 13, 14)
# ─────────────────────────────────────────

# Ex 8: Fill null amounts with 0.0
# Hint: fillna() or F.coalesce()


# Ex 9: Fill null tier with "standard"
# Hint: fillna() on specific column


# Ex 10: Add a column amount_category:
#   amount > 200 → "high"
#   amount between 100 and 200 → "medium"
#   amount < 100 → "low"
#   null amount → "unknown"
# Hint: F.when().when().when().otherwise()


# Ex 11: Add a column is_premium = True if tier == "premium", else False
# Hint: F.when().otherwise() or cast a boolean expression


# ─────────────────────────────────────────
# SET OPERATIONS (Easy 15)
# ─────────────────────────────────────────

# New small DataFrames for set ops
df_jan = df.filter(F.month("txn_date") == 1)   # January transactions
df_premium = df.filter(F.col("tier") == "premium")  # Premium customers

# Ex 12: Union df_jan and df_premium (keep duplicates)
# How many rows do you expect?


# Ex 13: Get rows that appear in BOTH df_jan AND df_premium
# Hint: intersect()


# Ex 14: Get rows in df_jan that are NOT in df_premium
# Hint: subtract()


# Ex 15: Remove duplicate rows from the union result (Ex 12)
# Hint: dropDuplicates() or distinct()

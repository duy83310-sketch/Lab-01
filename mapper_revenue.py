import sys
import csv

reader = csv.DictReader(sys.stdin)
for row in reader:
    if row.get("status", "").strip() == "SUCCESS":
        category = row.get("category", "").strip()
        amount = row.get("amount", "").strip()
        if category and amount:
            print(f"{category}\t{amount}")

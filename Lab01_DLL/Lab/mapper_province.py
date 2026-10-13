import sys
import csv

reader = csv.DictReader(sys.stdin)
for row in reader:
    if row.get("status", "").strip() == "SUCCESS":
        province = row.get("province", "").strip()
        amount = row.get("amount", "").strip()
        if province and amount:
            print(f"{province}\t{amount}")

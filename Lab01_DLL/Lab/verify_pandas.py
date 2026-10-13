import csv
from collections import defaultdict

totals = defaultdict(int)
counts = defaultdict(int)

with open("orders_2026_09.csv", "r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        if row.get("status", "").strip() == "SUCCESS":
            cat = row.get("category", "").strip()
            amt = int(row.get("amount", 0))
            totals[cat] += amt
            counts[cat] += 1

mapreduce_totals = {}
with open("part-00000", "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        cat, val = line.split("\t")
        mapreduce_totals[cat] = int(val)

is_identical = totals == mapreduce_totals
print("Verification Result:")
print("Are Python baseline and MapReduce output identical?", is_identical)

sorted_cats = sorted(totals.items(), key=lambda x: x[1], reverse=True)
print("\nSorted Revenue Comparison:")
print(f"{'Category':<15} | {'Orders':<8} | {'Python Revenue':<15} | {'MapReduce Revenue':<17} | {'Status'}")
print("-" * 75)
for cat, py_val in sorted_cats:
    mr_val = mapreduce_totals.get(cat, 0)
    status_str = "MATCH" if py_val == mr_val else "MISMATCH"
    print(f"{cat:<15} | {counts[cat]:<8} | {py_val:<15} | {mr_val:<17} | {status_str}")

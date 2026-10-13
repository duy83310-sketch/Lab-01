import sys
import csv

reader = csv.DictReader(sys.stdin)
for row in reader:
    channel = row.get("channel", "").strip()
    status = row.get("status", "").strip()
    if channel and status:
        is_failed = 1 if status == "FAILED" else 0
        print(f"{channel}\t{is_failed}\t1")

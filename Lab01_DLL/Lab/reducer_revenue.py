import sys

current_category = None
current_total = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split("\t")
    if len(parts) != 2:
        continue

    category, amount_str = parts
    try:
        amount = int(amount_str)
    except ValueError:
        continue

    if current_category == category:
        current_total += amount
    else:
        if current_category is not None:
            print(f"{current_category}\t{current_total}")
        current_category = category
        current_total = amount

if current_category is not None:
    print(f"{current_category}\t{current_total}")

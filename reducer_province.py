import sys

current_province = None
current_total = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split("\t")
    if len(parts) != 2:
        continue

    province, amount_str = parts
    try:
        amount = int(amount_str)
    except ValueError:
        continue

    if current_province == province:
        current_total += amount
    else:
        if current_province is not None:
            print(f"{current_province}\t{current_total}")
        current_province = province
        current_total = amount

if current_province is not None:
    print(f"{current_province}\t{current_total}")

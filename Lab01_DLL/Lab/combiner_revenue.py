import sys

current_key = None
current_total = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split("\t")
    if len(parts) != 2:
        continue

    key, val_str = parts
    try:
        val = int(val_str)
    except ValueError:
        continue

    if current_key == key:
        current_total += val
    else:
        if current_key is not None:
            print(f"{current_key}\t{current_total}")
        current_key = key
        current_total = val

if current_key is not None:
    print(f"{current_key}\t{current_total}")

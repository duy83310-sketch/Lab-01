import sys

current_channel = None
failed_count = 0
total_count = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split("\t")
    if len(parts) != 3:
        continue

    channel, failed_str, total_str = parts
    try:
        f = int(failed_str)
        t = int(total_str)
    except ValueError:
        continue

    if current_channel == channel:
        failed_count += f
        total_count += t
    else:
        if current_channel is not None:
            rate = (failed_count / total_count) * 100 if total_count > 0 else 0
            print(f"{current_channel}\t{failed_count}\t{total_count}\t{rate:.2f}%")
        current_channel = channel
        failed_count = f
        total_count = t

if current_channel is not None:
    rate = (failed_count / total_count) * 100 if total_count > 0 else 0
    print(f"{current_channel}\t{failed_count}\t{total_count}\t{rate:.2f}%")

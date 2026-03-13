import datetime

timestamp = datetime.datetime.now()

with open("data.txt", "a") as f:
    f.write(f"Parser ran at: {timestamp}\n")

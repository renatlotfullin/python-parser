import datetime

with open("data.txt", "w") as f:
    f.write("Parser ran at: " + str(datetime.datetime.now()))

import numpy
from sys import stdin

lengths = []
longest = ""
llen = 0

for line in stdin:
    lengths.append(len(line) - 1)
    if lengths[-1] > llen:
        longest = line[:-1]
        llen = lengths[-1]

print(longest,file=open("longest_line.txt","w"))
print("Average line length in chars: %.2f" % numpy.mean(lengths))
print("Median line length in chars: %.2f" % numpy.median(lengths))
print("Longest line in chars: %u" % max(lengths))

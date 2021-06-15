import numpy
from sys import stdin

lengths = []

for line in stdin:
    lengths.append(len(line) - 1)

print("Average line length in chars: %.2f" % numpy.mean(lengths))
print("Median line length in chars: %.2f" % numpy.median(lengths))
print("Longest line in chars: %u" % max(lengths))

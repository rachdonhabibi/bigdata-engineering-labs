#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    words = line.split()
    for word in words:
        print(f"{word}\t1")
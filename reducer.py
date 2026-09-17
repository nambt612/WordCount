#!/usr/bin/env python3
"""
reducer.py — Hadoop Streaming reducer
Doc cap "Nhom\tso_luong" da duoc sort theo key tu Hadoop shuffle,
gom nhom lien tiep va cong don so luong.
"""
import sys

current_category = None
current_count = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        category, count = line.split("\t", 1)
        count = int(count)
    except ValueError:
        continue

    if current_category == category:
        current_count += count
    else:
        if current_category is not None:
            print(f"{current_category}\t{current_count}")
        current_category = category
        current_count = count

if current_category is not None:
    print(f"{current_category}\t{current_count}")

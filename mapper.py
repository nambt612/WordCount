#!/usr/bin/env python3
"""
mapper.py — Hadoop Streaming mapper
Doc tung dong tu stdin, tach tu, phan loai theo do dai, in ra "Nhom\t1"
"""
import sys
import re


def classify(word):
    word = re.sub(r'[^a-zA-Z0-9À-ỹ]', '', word)
    if not word:
        return None
    length = len(word)
    if length == 1:
        return "Rat nho"
    elif 2 <= length <= 4:
        return "Nho"
    elif 5 <= length <= 9:
        return "Trung binh"
    else:
        return "Lon"


for line in sys.stdin:
    for word in line.strip().split():
        category = classify(word)
        if category:
            print(f"{category}\t1")

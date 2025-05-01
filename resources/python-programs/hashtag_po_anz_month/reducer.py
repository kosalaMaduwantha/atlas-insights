#!/usr/bin/env python3
# filepath: /home/kosala/git-repos/sentiment-anz-hadoop/resources/python-programs/reducer.py

import sys
from collections import defaultdict

def main():
    """read input line from stdin
    and output the year-month, hashtag, and aggregated frequency
    in tab-separated format to stdout
    """
    current_key = None
    current_sum = 0

    for line in sys.stdin:
        try:
            key, freq = line.strip().rsplit("\t", 1)
            freq = int(freq)

            if current_key == key:
                current_sum += freq
            else:
                if current_key:
                    print(f"{current_key}\t{current_sum}")
                current_key = key
                current_sum = freq
        except Exception as e:
            sys.stderr.write(f"Error processing line: {line}\n{e}\n")

    # Emit the last key
    if current_key:
        print(f"{current_key}\t{current_sum}")

if __name__ == "__main__":
    main()
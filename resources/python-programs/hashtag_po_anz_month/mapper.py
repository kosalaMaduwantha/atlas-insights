#!/usr/bin/env python3
# filepath: /home/kosala/git-repos/sentiment-anz-hadoop/resources/python-programs/mapper.py

import sys

def main():
    """read input line from stdin
    and output the year-month, hashtag, and frequency
    in tab-separated format to stdout
    
    """
    for line in sys.stdin:
        if "id,date,hash_tag,freq" in line:
            continue
        
        fields = line.strip().split(",")
        if len(fields) >= 4:
            try:
                date = fields[1]
                year_month = "-".join(date.split("-")[:2])  
                hashtag = fields[2]
                freq = int(fields[3].strip())
                print(f"{year_month}\t{hashtag}\t{freq}")
            except Exception as e:
                sys.stderr.write(f"Error processing line: {line}\n{e}\n")

if __name__ == "__main__":
    main()
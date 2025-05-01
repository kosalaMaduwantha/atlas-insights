#!/usr/bin/env python3

import sys
import re
import csv
from datetime import datetime

URL_PATTERN = re.compile(r"https?://\S+")
HASHTAG_PATTERN = re.compile(r"#(\w+)")
SPECIAL_CHAR_PATTERN = re.compile(r"[^\w\s#]")

INPUT_DATE_FORMAT = "%Y%m%d"
OUTPUT_DATE_FORMAT = "%Y-%m-%d"

def clean_text(text):
    text = URL_PATTERN.sub("", text)
    text = SPECIAL_CHAR_PATTERN.sub("", text)
    return text

def parse_date(timestamp):
    try:
        parsed_date = datetime.strptime(timestamp, INPUT_DATE_FORMAT)
        return parsed_date.strftime(OUTPUT_DATE_FORMAT)
    except Exception:
        return "unknown"

def process_line(line):
    """process each line of the input data
    1. Split the line by comma
    2. Extract the relevant fields
    3. Clean the text by removing URLs and special characters
    4. Parse the date from the timestamp
    5. Extract hashtags from the text
    6. Return the id, date, and hashtags

    Args:
        line (str): A single line of input data.

    Returns:
        tuple: A tuple containing the id, date, and list of hashtags.
    """
    fields = line.strip().split(",")
    if len(fields) < 4:
        return None 

    id = fields[1]
    timestamp = fields[2].replace('"', '')
    text = fields[3].replace('"', '')

    text = clean_text(text)
    date = parse_date(timestamp)
    hashtags = HASHTAG_PATTERN.findall(text)
    return id, date, hashtags

def main():
    writer = csv.writer(sys.stdout)
    
    for line in sys.stdin:
        result = process_line(line)
        if result:
            id, date, hashtags = result
            for hashtag in hashtags:
                writer.writerow([id, date, f"#{hashtag.lower()}", 1])
        else:
            pass # Ignore malformed lines

if __name__ == "__main__":
    main()
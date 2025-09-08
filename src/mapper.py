#!/usr/bin/env python3
"""
Mapper for E-commerce Transaction Analysis
Processes CSV data and emits (country, 1) pairs for counting transactions per country
"""

import sys
import logging
import os

# Configure logging to write to stderr (Hadoop captures stdout for data flow)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [MAPPER] %(levelname)s: %(message)s',
    stream=sys.stderr
)

def main():
    logging.info("Starting Mapper process")
    line_count = 0
    processed_count = 0
    
    for line in sys.stdin:
        line_count += 1
        line = line.strip()
        
        # Skip empty lines or header
        if not line or line.startswith('InvoiceNo'):
            logging.debug(f"Skipping line {line_count}: header or empty")
            continue
            
        try:
            # Split CSV line (assuming comma-separated)
            parts = line.split(',')
            
            # Ensure we have enough columns (Country should be the last column)
            if len(parts) < 8:
                logging.warning(f"Line {line_count}: Insufficient columns, skipping")
                continue
                
            # Extract country (last column)
            country = parts[-1].strip().strip('"')
            
            if country and country != '':
                # Emit key-value pair: country\t1
                print(f"{country}\t1")
                processed_count += 1
                logging.debug(f"Emitted: ({country}, 1)")
                
                # Log every 1000 processed records
                if processed_count % 1000 == 0:
                    logging.info(f"Processed {processed_count} records so far")
            else:
                logging.warning(f"Line {line_count}: Empty country field, skipping")
                
        except Exception as e:
            logging.error(f"Error processing line {line_count}: {e}")
            continue
    
    logging.info(f"Mapper completed. Total lines: {line_count}, Processed: {processed_count}")

if __name__ == "__main__":
    main()

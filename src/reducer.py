#!/usr/bin/env python3
"""
Reducer for E-commerce Transaction Analysis
Aggregates transaction counts per country from mapper output
"""

import sys
import logging

# Configure logging to write to stderr (Hadoop captures stdout for data flow)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [REDUCER] %(levelname)s: %(message)s',
    stream=sys.stderr
)

def main():
    logging.info("Starting Reducer process")
    
    current_country = None
    current_count = 0
    total_countries = 0
    total_transactions = 0
    
    for line in sys.stdin:
        line = line.strip()
        
        if not line:
            continue
            
        try:
            # Parse mapper output: country\tcount
            country, count = line.split('\t')
            count = int(count)
            
            if current_country == country:
                # Same country, accumulate count
                current_count += count
                logging.debug(f"Adding {count} to {country}, total now: {current_count}")
            else:
                # Different country, output previous country's total
                if current_country is not None:
                    print(f"{current_country}\t{current_count}")
                    total_countries += 1
                    total_transactions += current_count
                    logging.info(f"Country: {current_country}, Total transactions: {current_count}")
                
                # Start new country
                current_country = country
                current_count = count
                logging.debug(f"Starting new country: {country} with count: {count}")
                
        except ValueError as e:
            logging.error(f"Error parsing line '{line}': {e}")
            continue
        except Exception as e:
            logging.error(f"Unexpected error processing line '{line}': {e}")
            continue
    
    # Output the last country
    if current_country is not None:
        print(f"{current_country}\t{current_count}")
        total_countries += 1
        total_transactions += current_count
        logging.info(f"Country: {current_country}, Total transactions: {current_count}")
    
    logging.info(f"Reducer completed. Total countries: {total_countries}, Total transactions: {total_transactions}")

if __name__ == "__main__":
    main()

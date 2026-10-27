#!/usr/bin/env python3
"""
Hadoop Streaming Mapper: Store-Wise Sales
Reads retail transaction records from standard input (stdin)
and outputs Key-Value pairs in the format: <Store>\t<Sales_Amount>
"""
import sys
import csv

def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        
        # Skip CSV Header if present
        if line.lower().startswith("date") or line.lower().startswith("transaction_id"):
            continue
        
        # Parse delimiter (supports comma or tab)
        delimiter = ',' if ',' in line else '\t'
        parts = line.split(delimiter)
        
        # Expected formats:
        # 1. Standard format (6 columns): date, time, store, category, sales_amount, payment_method
        # 2. Udacity classic format (6 columns): date, time, store, category, sales_amount, payment_method
        # 3. Simple format (2 columns): store, sales_amount
        try:
            if len(parts) >= 6:
                store = parts[2].strip()
                sales_amount = float(parts[4].strip())
            elif len(parts) >= 5:
                store = parts[2].strip()
                sales_amount = float(parts[4].strip())
            elif len(parts) == 2:
                store = parts[0].strip()
                sales_amount = float(parts[1].strip())
            else:
                continue
            
            # Emit Key-Value pair to stdout for Hadoop Shuffle & Sort
            print(f"{store}\t{sales_amount:.2f}")
        except (ValueError, IndexError):
            # Ignore malformed or non-numeric sales rows
            continue

if __name__ == "__main__":
    main()

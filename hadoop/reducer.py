#!/usr/bin/env python3
"""
Hadoop Streaming Reducer: Store-Wise Sales Totals
Reads sorted Key-Value pairs (<Store>\t<Sales_Amount>) from standard input (stdin),
aggregates the sales amount per store, and outputs: <Store>\t<Total_Sales>
"""
import sys

def main():
    current_store = None
    current_total_sales = 0.0
    current_count = 0

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        
        try:
            store, sales_str = line.split('\t', 1)
            sales_amount = float(sales_str)
        except ValueError:
            # Skip invalid lines
            continue
        
        # In Hadoop MapReduce, input to Reducer is sorted by Key (store)
        if current_store == store:
            current_total_sales += sales_amount
            current_count += 1
        else:
            if current_store is not None:
                # Output previous store's aggregated sales
                print(f"{current_store}\t{current_total_sales:.2f}")
            current_store = store
            current_total_sales = sales_amount
            current_count = 1

    # Emit the last store
    if current_store is not None:
        print(f"{current_store}\t{current_total_sales:.2f}")

if __name__ == "__main__":
    main()

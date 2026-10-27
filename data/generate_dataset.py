#!/usr/bin/env python3
"""
Generate realistic synthetic retail transaction data
Columns: date, time, store, category, sales_amount, payment_method
"""
import random
import csv
import datetime
from pathlib import Path

STORES = [
    "New York Flagship",
    "San Francisco Downtown",
    "Chicago Central",
    "Los Angeles Plaza",
    "Austin Tech-Ridge",
    "Seattle Pike-Place",
    "Miami Beachwalk",
    "Boston Commons",
    "Atlanta Midtown",
    "Denver Galleria",
    "Dallas Metro",
    "Phoenix Desert-Ridge"
]

CATEGORIES = [
    "Electronics",
    "Clothing & Apparel",
    "Groceries & Food",
    "Home & Kitchen",
    "Books & Media",
    "Sports & Outdoors",
    "Health & Beauty",
    "Toys & Games"
]

PAYMENT_METHODS = ["Credit Card", "Debit Card", "Cash", "Mobile Pay (UPI/Apple Pay)"]

CATEGORY_PRICE_RANGES = {
    "Electronics": (45.0, 1250.0),
    "Clothing & Apparel": (15.0, 220.0),
    "Groceries & Food": (4.50, 160.0),
    "Home & Kitchen": (18.0, 450.0),
    "Books & Media": (8.0, 85.0),
    "Sports & Outdoors": (25.0, 390.0),
    "Health & Beauty": (10.0, 140.0),
    "Toys & Games": (12.0, 120.0),
}

def generate_transactions(num_records=1000, output_file="retail_transactions.csv"):
    start_date = datetime.date(2026, 1, 1)
    end_date = datetime.date(2026, 3, 31)
    time_delta = (end_date - start_date).days

    records = []
    for _ in range(num_records):
        random_day = start_date + datetime.timedelta(days=random.randint(0, time_delta))
        random_time = f"{random.randint(9, 21):02d}:{random.randint(0, 59):02d}"
        
        # Weighted store selection so sales figures look realistic
        store = random.choice(STORES)
        category = random.choice(CATEGORIES)
        min_p, max_p = CATEGORY_PRICE_RANGES[category]
        amount = round(random.uniform(min_p, max_p), 2)
        payment = random.choice(PAYMENT_METHODS)

        records.append([
            random_day.strftime("%Y-%m-%d"),
            random_time,
            store,
            category,
            f"{amount:.2f}",
            payment
        ])

    filepath = Path(output_file)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    
    with open(filepath, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "time", "store", "category", "sales_amount", "payment_method"])
        writer.writerows(records)

    print(f"Generated {num_records} transaction records in {filepath.resolve()}")
    return filepath

if __name__ == "__main__":
    import sys
    count = int(sys.argv[1]) if len(sys.argv) > 1 else 1000
    out = sys.argv[2] if len(sys.argv) > 2 else "retail_transactions.csv"
    generate_transactions(count, out)

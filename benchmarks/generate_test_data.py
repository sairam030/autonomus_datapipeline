#!/usr/bin/env python3
"""
Synthetic Data Generator for Benchmark Tests

Generates CSV files at different record counts (10K, 50K, 100K, 500K)
using the same schema as routes.csv (FlightNo, Origin, Destination, ScheduledDepartureTime).
"""

import csv
import os
import random
import string
import argparse

# Airports — realistic IATA codes
AIRPORTS = [
    "DEL", "BOM", "BLR", "HYD", "MAA", "CCU", "AMD", "PNQ", "GOI", "COK",
    "JAI", "LKO", "IXC", "NAG", "PAT", "GAU", "BBI", "IXR", "VTZ", "TRV",
    "SXR", "IXJ", "ATQ", "IDR", "RPR", "VNS", "JDH", "UDR", "IXB", "IXA",
    "DIB", "IXS", "IXE", "CJB", "TRZ", "CCJ", "IXM", "BHO", "GWL", "DED",
    "IXL", "DHM", "KNU", "BDQ", "STV", "HBX", "JSA", "AJL", "IMF", "IXZ",
]

# Airlines with realistic flight number prefixes
AIRLINE_PREFIXES = ["6E", "AI", "UK", "SG", "G8", "QP", "I5", "S5"]


def generate_flight_number():
    prefix = random.choice(AIRLINE_PREFIXES)
    number = random.randint(100, 9999)
    return f"{prefix}{number}"


def generate_time():
    hour = random.randint(0, 23)
    minute = random.choice([0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55])
    return f"{hour:02d}:{minute:02d}"


def generate_csv(filepath: str, num_records: int):
    """Generate a CSV file with the specified number of flight route records."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["FlightNo", "Origin", "Destination", "ScheduledDepartureTime"])

        for _ in range(num_records):
            origin = random.choice(AIRPORTS)
            dest = random.choice([a for a in AIRPORTS if a != origin])
            writer.writerow([
                generate_flight_number(),
                origin,
                dest,
                generate_time(),
            ])

    size_kb = os.path.getsize(filepath) / 1024
    print(f"  ✅ Generated {filepath} — {num_records:,} records ({size_kb:.1f} KB)")


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic benchmark CSV data")
    parser.add_argument("--output-dir", default="benchmarks/data",
                        help="Output directory (default: benchmarks/data)")
    parser.add_argument("--sizes", nargs="+", type=int,
                        default=[10000, 50000, 100000, 500000],
                        help="Record counts to generate (default: 10000 50000 100000 500000)")
    args = parser.parse_args()

    print("🔧 Generating synthetic benchmark data...\n")

    for size in args.sizes:
        label = f"{size // 1000}K" if size < 1_000_000 else f"{size // 1_000_000}M"
        filepath = os.path.join(args.output_dir, f"routes_{label}.csv")
        generate_csv(filepath, size)

    print(f"\n✅ Done! Generated {len(args.sizes)} files in {args.output_dir}/")


if __name__ == "__main__":
    main()

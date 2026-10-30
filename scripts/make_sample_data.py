#!/usr/bin/env python3
# /// script
# dependencies = [
#   "numpy",
#   "pandas",
#   "pyarrow",
# ]
# ///
"""Generate a small synthetic dataset with the same layout as the city-M files.

- speed / volume: one `timestamp` column (5 minute steps) and one `node_$n`
  column per road segment, with a few missing values.
- static features: one row per node, row n (1-based) describes node_n.
"""

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def parse_args():
    parser = argparse.ArgumentParser(description="Generate synthetic traffic data.")
    parser.add_argument("--out", default="dataset", help="Output directory.")
    parser.add_argument("--measurements", type=int, default=2000, help="Number of 5 minute steps.")
    parser.add_argument("--segments", type=int, default=100, help="Number of node columns.")
    parser.add_argument("--total-nodes", type=int, default=500, help="Number of rows in the static file.")
    parser.add_argument("--null-rate", type=float, default=0.02, help="Fraction of missing values.")
    parser.add_argument("--seed", type=int, default=42)
    return parser.parse_args()


def main():
    args = parse_args()
    rng = np.random.default_rng(args.seed)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    n_total = args.total_nodes
    static = pd.DataFrame({
        "length": rng.uniform(50, 2000, n_total),
        "lanes": rng.integers(1, 5, n_total).astype(float),
        "speed_limit": rng.choice([30.0, 50.0, 70.0, 90.0], n_total),
        "category": rng.integers(0, 6, n_total).astype(float),
    })

    # Like the real subsets, the columns are neither contiguous nor sorted.
    nodes = rng.choice(np.arange(1, n_total + 1), args.segments, replace=False)
    timestamps = pd.date_range("2025-01-06", periods=args.measurements, freq="5min")
    minutes = timestamps.hour.to_numpy() * 60 + timestamps.minute.to_numpy()
    rush = np.exp(-((minutes - 8 * 60) / 60.0) ** 2) + np.exp(-((minutes - 17.5 * 60) / 75.0) ** 2)

    speed = {"timestamp": timestamps}
    volume = {"timestamp": timestamps}
    for n in nodes:
        row = static.iloc[n - 1]
        free_flow = row["speed_limit"] * rng.uniform(0.8, 1.0)
        congestion = rng.uniform(0.2, 0.6)
        noise = np.cumsum(rng.normal(0, 0.5, args.measurements)) * 0.1
        s = free_flow * (1 - congestion * rush) + noise + rng.normal(0, 1.5, args.measurements)
        v = row["lanes"] * (2 + 6 * rush) + rng.normal(0, 0.4, args.measurements)
        s[rng.random(args.measurements) < args.null_rate] = np.nan
        v[rng.random(args.measurements) < args.null_rate] = np.nan
        speed[f"node_{n}"] = np.clip(s, 0, None)
        volume[f"node_{n}"] = np.clip(v, 0, None)

    suffix = f"{args.measurements}_{args.segments}"
    pd.DataFrame(speed).to_parquet(out / f"city_M_speed_{suffix}.parquet", index=False)
    pd.DataFrame(volume).to_parquet(out / f"city_M_volume_{suffix}.parquet", index=False)
    static.to_parquet(out / "city_M_static_features.parquet", index=False)
    print(f"Wrote {args.measurements} x {args.segments} measurements and {n_total} static rows to {out}/")


if __name__ == "__main__":
    main()

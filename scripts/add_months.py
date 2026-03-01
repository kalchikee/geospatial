"""
Generate 2 additional months (2024-08, 2024-09) for the static docs/data.
Reads the 2024-07 geojson as a base and applies realistic seasonal NDVI offsets.
August: slight increase (peak summer), September: slight decline (late summer).
"""
import json
import random
import math
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "docs" / "data"

# Seasonal offset per new month (relative to 2024-07 values)
NEW_MONTHS = [
    ("2024-08", 0.008),   # August: slight uptick (peak summer greenness)
    ("2024-09", -0.018),  # September: moderate decline (late summer)
]

SOURCE = "sentinel2"


def jitter(val, scale=0.003):
    """Small random noise to avoid identical values."""
    return val + random.gauss(0, scale)


def clamp(val, lo=-1.0, hi=1.0):
    return max(lo, min(hi, val))


def generate_month(base_period: str, new_period: str, ndvi_offset: float):
    base_path = DATA_DIR / f"parcels-{base_period}-{SOURCE}.geojson"
    with open(base_path) as f:
        fc = json.load(f)

    ndvi_vals = []
    for feat in fc["features"]:
        p = feat["properties"]
        old_val = p.get("ndvi_mean")
        if old_val is not None:
            new_val = clamp(jitter(old_val + ndvi_offset))
            p["ndvi_mean"] = round(new_val, 4)
            ndvi_vals.append(new_val)
        p["period"] = new_period

    # Write parcels geojson
    out_path = DATA_DIR / f"parcels-{new_period}-{SOURCE}.geojson"
    with open(out_path, "w") as f:
        json.dump(fc, f, separators=(",", ":"))
    print(f"  Wrote {out_path.name}  ({len(fc['features'])} features)")

    # Write empty changes geojson (no detections for synthetic months)
    changes_fc = {"type": "FeatureCollection", "features": []}
    changes_path = DATA_DIR / f"changes-{new_period}-{SOURCE}.geojson"
    with open(changes_path, "w") as f:
        json.dump(changes_fc, f)
    print(f"  Wrote {changes_path.name}")

    # Compute stats
    n = len(ndvi_vals)
    city_mean   = sum(ndvi_vals) / n
    sorted_vals = sorted(ndvi_vals)
    mid = n // 2
    city_median = (sorted_vals[mid - 1] + sorted_vals[mid]) / 2 if n % 2 == 0 else sorted_vals[mid]
    city_min    = min(ndvi_vals)
    city_max    = max(ndvi_vals)
    variance    = sum((v - city_mean) ** 2 for v in ndvi_vals) / n
    city_std    = math.sqrt(variance)

    stats = {
        "period":        new_period,
        "source":        SOURCE,
        "parcel_count":  n,
        "city_mean":     city_mean,
        "city_median":   city_median,
        "city_min":      city_min,
        "city_max":      city_max,
        "city_std":      city_std,
        "high_veg_count": sum(1 for v in ndvi_vals if v >= 0.3),
        "low_veg_count":  sum(1 for v in ndvi_vals if v < 0.3),
    }
    stats_path = DATA_DIR / f"stats-{new_period}-{SOURCE}.json"
    with open(stats_path, "w") as f:
        json.dump(stats, f)
    print(f"  Wrote {stats_path.name}  mean={city_mean:.4f}")

    return {"period": new_period, "source": SOURCE}


def update_periods(new_entries):
    periods_path = DATA_DIR / "periods.json"
    with open(periods_path) as f:
        periods = json.load(f)

    existing = {(p["period"], p["source"]) for p in periods}
    added = []
    for entry in new_entries:
        key = (entry["period"], entry["source"])
        if key not in existing:
            periods.append(entry)
            added.append(entry["period"])

    # Sort descending
    periods.sort(key=lambda p: p["period"], reverse=True)

    with open(periods_path, "w") as f:
        json.dump(periods, f, separators=(",", ":"))
    print(f"  periods.json updated — added: {added}")


def update_history(new_periods):
    """Append new month entries into history.json for all parcels."""
    history_path = DATA_DIR / "history.json"
    with open(history_path) as f:
        history = json.load(f)

    for new_period, ndvi_offset in new_periods:
        # Read the newly created parcels file
        parcels_path = DATA_DIR / f"parcels-{new_period}-{SOURCE}.geojson"
        with open(parcels_path) as f:
            fc = json.load(f)

        for feat in fc["features"]:
            p = feat["properties"]
            pin = str(p.get("pin", ""))
            if not pin:
                continue
            if pin not in history:
                history[pin] = []
            # Avoid duplicates
            if any(h["period"] == new_period for h in history[pin]):
                continue
            history[pin].append({"period": new_period, "ndvi_mean": p["ndvi_mean"]})

    # Sort each parcel's history chronologically
    for pin in history:
        history[pin].sort(key=lambda h: h["period"])

    with open(history_path, "w") as f:
        json.dump(history, f, separators=(",", ":"))
    total = sum(len(v) for v in history.values())
    print(f"  history.json updated — {len(history)} parcels, {total} total points")


if __name__ == "__main__":
    random.seed(42)
    new_entries = []
    for new_period, offset in NEW_MONTHS:
        print(f"\nGenerating {new_period} (offset={offset:+.3f})…")
        entry = generate_month("2024-07", new_period, offset)
        new_entries.append(entry)

    print("\nUpdating periods.json…")
    update_periods(new_entries)

    print("\nUpdating history.json…")
    update_history(NEW_MONTHS)

    print("\nDone.")

from __future__ import annotations
import re
from typing import Literal

# 1) Payment fee rates
PAYMENT_FEE_RATE = {
    "card": 0.025,  # 2.5%
    "credit card": 0.025,
    "debit card": 0.015,
    "bank transfer": 0.008,  # 0.8%
    "transfer": 0.008,
    "cash": 0.0,
}


def fee_rate_for(method: str | None) -> float:
    if not method:
        return 0.0
    m = method.strip().lower()
    return next((rate for key, rate in PAYMENT_FEE_RATE.items() if key in m), 0.0)


# 2) Product category detection (Parts vs Bikes)
PARTS_KEYWORDS = [
    "part",
    "tire",
    "tyre",
    "tube",
    "rim",
    "spoke",
    "brake",
    "pad",
    "disc",
    "rotor",
    "chain",
    "sprocket",
    "cassette",
    "derailleur",
    "gear",
    "cable",
    "grip",
    "pedal",
    "saddle",
    "seat",
    "stem",
    "handle",
    "bar",
    "fork",
    "shock",
    "suspension",
    "helmet",
    "glove",
    "light",
    "accessory",
    "pump",
    "lock",
]
PARTS_RE = re.compile("|".join(PARTS_KEYWORDS), re.IGNORECASE)


def categorize_product(name: str | None) -> Literal["Parts", "Bikes"]:
    if not name:
        return "Bikes"
    return "Parts" if PARTS_RE.search(name) else "Bikes"


# 3) Warehouse mapping from Store_Location (city/state strings → region)
REGION_KEYWORDS = {
    "West": [" CA", " WA", " OR", " NV", "AZ", "UT", "ID", "MT", "WY", "CO", "NM"],
    "East": [
        " NY",
        " NJ",
        " MA",
        " PA",
        " MD",
        " DC",
        " VA",
        "NC",
        "SC",
        "GA",
        "FL",
        "CT",
        "RI",
        "VT",
        "NH",
        "ME",
    ],
    "North": [" IL", " MI", " WI", " MN", " ND", " SD", " IA", " IN", " OH", " MO", "KS", "NE"],
    "South": [" TX", " OK", " LA", " AR", " MS", " AL", " TN", " KY"],
}


def warehouse_region(store_location: str | None) -> str:
    if not store_location:
        return "East"
    s = " " + store_location.upper()
    for region, tokens in REGION_KEYWORDS.items():
        if any(tok in s for tok in tokens):
            return region
    # fallback bucket
    return "East"

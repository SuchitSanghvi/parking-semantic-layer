"""
generate_raw_data.py
Generates three synthetic CSV files for the parking semantic layer demo:
  data/raw_parking_events.csv
  data/raw_lots.csv
  data/raw_local_events.csv

Run from the project root:
  python generate_data/generate_raw_data.py
"""

import csv
import os
import random
from datetime import datetime, timedelta
from typing import Optional

random.seed(42)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ---------------------------------------------------------------------------
# Lot master data (intentional messiness per spec)
# ---------------------------------------------------------------------------

LOTS = [
    {
        "lot_id": "LOT_SF_01", "lot_name": "Mission St Garage",
        "city": "San Francisco", "state": "CA", "zip": "94103",
        "capacity": 120, "market_type": "urban",
        "activation_date": "2022-03-15", "owner_name": "Bay Area Parking LLC",
    },
    {
        "lot_id": "LOT_SF_02", "lot_name": "SoMa Surface Lot",
        "city": "san francisco", "state": "CA", "zip": "94105",
        "capacity": 60, "market_type": "Urban",
        "activation_date": "01/20/2021", "owner_name": "Urban Holdings Inc",
    },
    {
        "lot_id": "LOT_SF_03", "lot_name": "Civic Center Plaza",
        "city": "SAN FRANCISCO", "state": "CA", "zip": "94102",
        "capacity": None, "market_type": "URBAN",           # NULL capacity
        "activation_date": "2020-07-01", "owner_name": "Civic Properties",
    },
    {
        "lot_id": "LOT_LA_01", "lot_name": "Downtown LA Garage",
        "city": "Los Angeles", "state": "CA", "zip": "90012",
        "capacity": 200, "market_type": "urban",
        "activation_date": "2021-11-10", "owner_name": "SoCal Park Co",
    },
    {
        "lot_id": "LOT_LA_02", "lot_name": "Hollywood Blvd Lot",
        "city": "los angeles", "state": "CA", "zip": "90028",
        "capacity": 90, "market_type": "mixed-use",
        "activation_date": "03/05/2022", "owner_name": "Sunset Ventures",
    },
    {
        "lot_id": "LOT_LA_03", "lot_name": "LAX Economy Parking",
        "city": "LOS ANGELES", "state": "CA", "zip": "90045",
        "capacity": 300, "market_type": "Mixed Use",
        "activation_date": "2019-06-01", "owner_name": "Airport Partners",
    },
    {
        "lot_id": "LOT_SEA_01", "lot_name": "Pike Place Market Lot",
        "city": "Seattle", "state": "WA", "zip": "98101",
        "capacity": 80, "market_type": "suburban",
        "activation_date": "2023-01-15", "owner_name": "PNW Parking Group",
    },
    {
        "lot_id": "LOT_SEA_02", "lot_name": "Capitol Hill Surface Lot",
        "city": "Seattle", "state": "WA", "zip": "98102",
        "capacity": 50, "market_type": "suburban",
        "activation_date": "09/30/2022", "owner_name": "Hill Properties LLC",
    },
]

LOT_IDS = [lot["lot_id"] for lot in LOTS]

LOT_CITY = {lot["lot_id"]: lot["city"] for lot in LOTS}

START_DATE = datetime(2024, 1, 1)
END_DATE   = datetime(2024, 3, 31, 23, 59, 59)
TOTAL_DAYS = 91


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def random_ts(start: datetime, end: datetime) -> datetime:
    delta = int((end - start).total_seconds())
    return start + timedelta(seconds=random.randint(0, delta))


def weighted_hour(is_weekend: bool) -> int:
    if is_weekend:
        weights = [1, 1, 1, 1, 1, 1, 2, 3, 5, 8, 10, 12, 13, 12, 11, 10, 10, 11, 10, 8, 6, 4, 2, 1]
    else:
        weights = [1, 1, 1, 1, 1, 1, 2, 5, 9, 8,  7,  8,  9,  8,  7,  7,  8,  7,  5, 3, 2, 2, 1, 1]
    return random.choices(range(24), weights=weights, k=1)[0]


def is_peak_pricing(ts: datetime) -> bool:
    """Fri evening, Sat all day, Sun afternoon → dynamic pricing trigger."""
    wd = ts.weekday()   # 0=Mon … 6=Sun
    h  = ts.hour
    return (wd == 4 and h >= 17) or (wd == 5) or (wd == 6 and h >= 12)


BASE_PRICES = {
    "LOT_SF_01": 8.0,  "LOT_SF_02": 6.0,  "LOT_SF_03": 7.0,
    "LOT_LA_01": 9.0,  "LOT_LA_02": 7.5,  "LOT_LA_03": 12.0,
    "LOT_SEA_01": 5.5, "LOT_SEA_02": 4.5,
}


def calc_price(lot_id: str, entry_ts: datetime, duration_min: int) -> float:
    price = BASE_PRICES.get(lot_id, 7.0)
    if is_peak_pricing(entry_ts):
        price *= random.uniform(1.30, 1.40)
    price *= (duration_min / 60.0) * random.uniform(0.8, 1.2)
    return max(round(price, 2), 1.0)


def make_license_plate() -> str:
    letters = "ABCDEFGHJKLMNPRSTUVWXYZ"
    digits  = "0123456789"
    return (
        random.choice(letters) + random.choice(letters) +
        random.choice(digits)  + random.choice(digits)  + random.choice(digits) +
        random.choice(letters) + random.choice(letters)
    )


def payment_method_value() -> Optional[str]:
    choices = ["app", "App", "APP", "web", "Web", "text", None]
    weights = [15,     10,    5,    12,    8,     8,    5]
    return random.choices(choices, weights=weights, k=1)[0]


def make_camera(lot_id: str) -> str:
    return f"CAM_{lot_id}_{random.randint(1, 4):02d}"


# ---------------------------------------------------------------------------
# raw_parking_events.csv
# ---------------------------------------------------------------------------

def build_parking_events() -> list:
    TARGET_SESSIONS  = 900
    rows: list       = []
    event_counter    = [1]

    def next_id() -> str:
        eid = f"EVT_{event_counter[0]:05d}"
        event_counter[0] += 1
        return eid

    def make_session_rows(lot_id: str, plate: str, entry_ts: datetime,
                          clock_bug: bool = False):
        duration_min = random.randint(15, 480)
        exit_ts = (entry_ts - timedelta(minutes=random.randint(1, 10))
                   if clock_bug
                   else entry_ts + timedelta(minutes=duration_min))

        price      = calc_price(lot_id, entry_ts, duration_min)
        pay_method = payment_method_value()
        amount_exit = "" if random.random() < 0.05 else str(price)
        pm_str      = pay_method if pay_method else ""

        entry = {
            "event_id": next_id(),
            "lot_id": lot_id,
            "license_plate": plate,
            "event_type": "ENTRY",
            "event_timestamp": entry_ts.strftime("%Y-%m-%d %H:%M:%S"),
            "amount_charged": "",
            "payment_method": pm_str,
            "camera_id": make_camera(lot_id),
        }
        exit_ = {
            "event_id": next_id(),
            "lot_id": lot_id,
            "license_plate": plate,
            "event_type": "EXIT",
            "event_timestamp": exit_ts.strftime("%Y-%m-%d %H:%M:%S"),
            "amount_charged": amount_exit,
            "payment_method": pm_str,
            "camera_id": make_camera(lot_id),
        }
        return entry, exit_

    plates_pool = [make_license_plate() for _ in range(400)]

    # ---- Normal sessions ----
    clock_bug_target = int(TARGET_SESSIONS * 0.01)
    clock_bugs_used  = 0

    for _ in range(TARGET_SESSIONS):
        lot_id = random.choice(LOT_IDS)
        plate  = random.choice(plates_pool)

        # Weight toward weekends
        day_offset = random.randint(0, TOTAL_DAYS - 1)
        base_day   = START_DATE + timedelta(days=day_offset)
        if base_day.weekday() < 5 and random.random() < 0.3:
            day_offset = random.randint(0, TOTAL_DAYS - 1)
            base_day   = START_DATE + timedelta(days=day_offset)

        hour = weighted_hour(base_day.weekday() >= 5)
        ts   = base_day.replace(hour=hour,
                                minute=random.randint(0, 59),
                                second=random.randint(0, 59))
        if ts > END_DATE:
            ts = ts.replace(hour=min(ts.hour, 22))

        use_clock_bug = clock_bugs_used < clock_bug_target and random.random() < 0.02
        entry, exit_ = make_session_rows(lot_id, plate, ts, clock_bug=use_clock_bug)
        if use_clock_bug:
            clock_bugs_used += 1

        rows.append(entry)
        rows.append(exit_)

    # ---- Inject ~3% duplicate ENTRY (camera misfire, no EXIT) ----
    misfire_target = int(TARGET_SESSIONS * 0.03)
    entry_rows     = [r for r in rows if r["event_type"] == "ENTRY"]
    for _ in range(misfire_target):
        original   = random.choice(entry_rows)
        dup        = dict(original)
        dup["event_id"] = next_id()
        orig_ts    = datetime.strptime(original["event_timestamp"], "%Y-%m-%d %H:%M:%S")
        dup["event_timestamp"] = (orig_ts + timedelta(seconds=random.randint(30, 119))).strftime("%Y-%m-%d %H:%M:%S")
        rows.append(dup)

    # ---- Inject ~2% orphaned EXIT (no matching ENTRY) ----
    for _ in range(int(TARGET_SESSIONS * 0.02)):
        lot_id = random.choice(LOT_IDS)
        plate  = make_license_plate()   # fresh plate → no ENTRY in dataset
        ts     = random_ts(START_DATE, END_DATE)
        price  = round(BASE_PRICES.get(lot_id, 7.0) * random.uniform(0.5, 2.0), 2)
        pm     = payment_method_value()
        rows.append({
            "event_id": next_id(),
            "lot_id": lot_id,
            "license_plate": plate,
            "event_type": "EXIT",
            "event_timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "amount_charged": str(price),
            "payment_method": pm if pm else "",
            "camera_id": make_camera(lot_id),
        })

    # ---- Inject 8-10 LOT_UNKNOWN rows ----
    for _ in range(random.randint(8, 10)):
        ts    = random_ts(START_DATE, END_DATE)
        etype = random.choice(["ENTRY", "EXIT"])
        price = "" if etype == "ENTRY" else str(round(random.uniform(3.0, 15.0), 2))
        pm    = payment_method_value()
        rows.append({
            "event_id": next_id(),
            "lot_id": "LOT_UNKNOWN",
            "license_plate": make_license_plate(),
            "event_type": etype,
            "event_timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "amount_charged": price,
            "payment_method": pm if pm else "",
            "camera_id": "CAM_UNKNOWN_01",
        })

    random.shuffle(rows)

    fieldnames = ["event_id", "lot_id", "license_plate", "event_type",
                  "event_timestamp", "amount_charged", "payment_method", "camera_id"]
    out_path = os.path.join(OUTPUT_DIR, "raw_parking_events.csv")
    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"raw_parking_events.csv  -> {len(rows):,} rows  ({out_path})")
    return rows


# ---------------------------------------------------------------------------
# raw_lots.csv
# ---------------------------------------------------------------------------

def build_lots() -> list:
    fieldnames = ["lot_id", "lot_name", "city", "state", "zip",
                  "capacity", "market_type", "activation_date", "owner_name"]
    out_path = os.path.join(OUTPUT_DIR, "raw_lots.csv")
    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for lot in LOTS:
            row = {k: ("" if lot[k] is None else lot[k]) for k in fieldnames}
            writer.writerow(row)

    print(f"raw_lots.csv            -> {len(LOTS):,} rows  ({out_path})")
    return LOTS


# ---------------------------------------------------------------------------
# raw_local_events.csv
# ---------------------------------------------------------------------------

def build_local_events() -> list:
    city_variants = {
        "San Francisco": ["San Francisco", "san francisco", "SAN FRANCISCO"],
        "Los Angeles":   ["Los Angeles",   "los angeles",   "LOS ANGELES"],
        "Seattle":       ["Seattle",       "seattle",       "SEATTLE"],
    }

    def cv(city: str) -> str:
        return random.choice(city_variants[city])

    def rand_date(month: int) -> str:
        days = {1: 31, 2: 29, 3: 31}
        return f"2024-{month:02d}-{random.randint(1, days[month]):02d}"

    events = [
        # Sports
        {"event_date": "2024-01-07",  "city": cv("San Francisco"), "event_name": "49ers Playoff Game",       "event_type": "Sports",   "expected_attendance": 68000},
        {"event_date": "2024-01-14",  "city": cv("Los Angeles"),   "event_name": "Lakers vs Celtics",        "event_type": "Sports",   "expected_attendance": 20000},
        {"event_date": "2024-01-21",  "city": cv("Seattle"),       "event_name": "Seahawks Home Game",       "event_type": "Sports",   "expected_attendance": 70000},
        {"event_date": "2024-02-04",  "city": cv("San Francisco"), "event_name": "Warriors vs Nuggets",      "event_type": "Sports",   "expected_attendance": 18000},
        {"event_date": "2024-02-18",  "city": cv("Los Angeles"),   "event_name": "Clippers Home Game",       "event_type": "Sports",   "expected_attendance": 19000},
        {"event_date": "2024-03-10",  "city": cv("Seattle"),       "event_name": "Sounders Home Opener",     "event_type": "Sports",   "expected_attendance": 40000},
        {"event_date": "2024-03-24",  "city": cv("San Francisco"), "event_name": "Giants Opening Day",       "event_type": "Sports",   "expected_attendance": 42000},
        # Concerts
        {"event_date": "2024-01-13",  "city": cv("Los Angeles"),   "event_name": "Taylor Swift Eras Tour",   "event_type": "Concert",  "expected_attendance": 55000},
        {"event_date": "2024-01-27",  "city": cv("San Francisco"), "event_name": "Beyonce Renaissance Tour", "event_type": "Concert",  "expected_attendance": 50000},
        {"event_date": "2024-02-10",  "city": cv("Seattle"),       "event_name": "Pearl Jam Homecoming",     "event_type": "Concert",  "expected_attendance": 45000},
        {"event_date": "2024-02-24",  "city": cv("Los Angeles"),   "event_name": "Bad Bunny World Tour",     "event_type": "Concert",  "expected_attendance": 60000},
        {"event_date": "2024-03-16",  "city": cv("San Francisco"), "event_name": "Coachella Preview Night",  "event_type": "Concert",  "expected_attendance": 25000},
        # Festivals
        {"event_date": "2024-01-20",  "city": cv("Los Angeles"),   "event_name": "LA Food & Wine Festival",  "event_type": "Festival", "expected_attendance": 12000},
        {"event_date": "2024-02-03",  "city": cv("Seattle"),       "event_name": "Seattle Beer Fest",        "event_type": "Festival", "expected_attendance": 8000},
        {"event_date": "2024-02-17",  "city": cv("San Francisco"), "event_name": "SF Street Food Fest",      "event_type": "Festival", "expected_attendance": 15000},
        {"event_date": "2024-03-02",  "city": cv("Los Angeles"),   "event_name": "Rose Bowl Flea Market",    "event_type": "Festival", "expected_attendance": 20000},
        {"event_date": "2024-03-09",  "city": cv("San Francisco"), "event_name": "Chinatown New Year Parade","event_type": "Festival", "expected_attendance": 30000},
        {"event_date": "2024-03-23",  "city": cv("Seattle"),       "event_name": "Sakura Festival",          "event_type": "Festival", "expected_attendance": 18000},
        # Additional filler
        {"event_date": rand_date(1),  "city": cv("San Francisco"), "event_name": "Oracle Park Concert",      "event_type": "Concert",  "expected_attendance": 40000},
        {"event_date": rand_date(2),  "city": cv("Los Angeles"),   "event_name": "Crypto.com Arena Event",   "event_type": "Sports",   "expected_attendance": 20000},
        {"event_date": rand_date(3),  "city": cv("Seattle"),       "event_name": "Seattle Marathon",         "event_type": "Festival", "expected_attendance": 10000},
        {"event_date": rand_date(1),  "city": cv("Los Angeles"),   "event_name": "Grammy Awards",            "event_type": "Concert",  "expected_attendance": 18000},
        {"event_date": rand_date(2),  "city": cv("San Francisco"), "event_name": "SF Marathon",              "event_type": "Festival", "expected_attendance": 25000},
        # Near-duplicates (duplicate data entry — same event, slightly different name)
        {"event_date": "2024-01-13",  "city": "los angeles",       "event_name": "Taylor Swift - Eras Tour", "event_type": "Concert",  "expected_attendance": 55000},
        {"event_date": "2024-02-04",  "city": "SAN FRANCISCO",     "event_name": "Warriors Vs Nuggets",      "event_type": "sports",   "expected_attendance": 18000},
    ]

    fieldnames = ["event_date", "city", "event_name", "event_type", "expected_attendance"]
    out_path = os.path.join(OUTPUT_DIR, "raw_local_events.csv")
    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(events)

    print(f"raw_local_events.csv    -> {len(events):,} rows  ({out_path})")
    return events


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("Generating synthetic parking data...\n")
    build_parking_events()
    build_lots()
    build_local_events()
    print("\nDone. Files written to data/")

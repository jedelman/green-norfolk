#!/usr/bin/env python3
"""
Fetch GIP compliance data from data.norfolk.gov and write JSON snapshots.
Runs via GitHub Actions, commits results to data/.
"""
import json, os, sys
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.parse import urlencode
from urllib.error import HTTPError

APP_TOKEN = os.environ.get("NORFOLK_APP_TOKEN", "")
BASE = "https://data.norfolk.gov/resource"

def query(dataset_id, params):
    qs = urlencode(params)
    url = f"{BASE}/{dataset_id}.json?{qs}"
    req = Request(url, headers={"X-App-Token": APP_TOKEN})
    with urlopen(req) as r:
        return json.loads(r.read())

def run():
    out = {"generated": datetime.now(timezone.utc).isoformat(), "datasets": {}}
    errors = []

    # ── 1. TREE PLANTING PROGRAM (u85v-sad9) ──────────────────────────────
    try:
        rows = query("u85v-sad9", {
            "$select": "program_year,count(*) as trees_planted",
            "$group": "program_year",
            "$order": "program_year"
        })
        TARGET = 5200
        years = []
        for r in rows:
            yr = r.get("program_year", "?")
            n = int(r.get("trees_planted", 0))
            years.append({"year": yr, "trees_planted": n, "target": TARGET, "shortfall": TARGET - n})
        avg = round(sum(y["trees_planted"] for y in years) / len(years)) if years else 0
        out["datasets"]["tree_planting_by_year"] = {
            "dataset_id": "u85v-sad9",
            "label": "Tree Planting Program — Annual Rate",
            "gip_benchmark": TARGET,
            "average_annual": avg,
            "meeting_benchmark": avg >= TARGET,
            "years": years
        }
        print(f"✓ tree_planting_by_year: {len(years)} years, avg {avg}/yr")
    except Exception as e:
        errors.append(f"tree_planting_by_year: {e}")
        print(f"✗ tree_planting_by_year: {e}")

    # ── 2. FORESTRY WORK ORDERS (qzfe-wj25) ───────────────────────────────
    try:
        # By task
        tasks = query("qzfe-wj25", {
            "$where": "area='Forestry'",
            "$select": "primary_task_description,count(*) as n",
            "$group": "primary_task_description",
            "$order": "n DESC"
        })
        # By year
        by_year = query("qzfe-wj25", {
            "$where": "area='Forestry'",
            "$select": "date_trunc_y(created_datetime) as yr,count(*) as n",
            "$group": "date_trunc_y(created_datetime)",
            "$order": "yr DESC",
            "$limit": "8"
        })
        removals = sum(int(t["n"]) for t in tasks if any(k in (t.get("primary_task_description","")).lower() for k in ["remov","grind","stump"]))
        plantings = sum(int(t["n"]) for t in tasks if any(k in (t.get("primary_task_description","")).lower() for k in ["plant","new tree"]))
        ratio = round(removals / plantings, 1) if plantings else None
        out["datasets"]["forestry_work_orders"] = {
            "dataset_id": "qzfe-wj25",
            "label": "Forestry Work Orders — Task Breakdown",
            "total_removals": removals,
            "total_plantings": plantings,
            "removal_to_planting_ratio": ratio,
            "net_canopy_direction": "loss" if (ratio and ratio > 1) else "gain" if plantings else "unknown",
            "tasks": [{"task": t.get("primary_task_description"), "count": int(t["n"])} for t in tasks],
            "by_year": [{"year": r.get("yr","?")[:4], "orders": int(r["n"])} for r in by_year]
        }
        print(f"✓ forestry_work_orders: removals={removals}, plantings={plantings}, ratio={ratio}")
    except Exception as e:
        errors.append(f"forestry_work_orders: {e}")
        print(f"✗ forestry_work_orders: {e}")

    # ── 3. CITY TREE INVENTORY (cmvv-agyb) ────────────────────────────────
    try:
        total_r = query("cmvv-agyb", {"$select": "count(*)"})
        total = int(total_r[0].get("count", 0))
        species = query("cmvv-agyb", {
            "$select": "common_name,count(*) as n",
            "$group": "common_name",
            "$order": "n DESC",
            "$limit": "12"
        })
        conditions = query("cmvv-agyb", {
            "$select": "condition,count(*) as n",
            "$group": "condition",
            "$order": "n DESC"
        })
        crepe = next((s for s in species if "crepe" in (s.get("common_name","")).lower() or "crape" in (s.get("common_name","")).lower()), None)
        crepe_pct = round(int(crepe["n"]) / total * 100, 1) if crepe and total else 0
        out["datasets"]["tree_inventory"] = {
            "dataset_id": "cmvv-agyb",
            "label": "City Tree Inventory",
            "total_trees": total,
            "crepe_myrtle_count": int(crepe["n"]) if crepe else 0,
            "crepe_myrtle_pct": crepe_pct,
            "top_species": [{"species": s.get("common_name"), "count": int(s["n"]), "pct": round(int(s["n"])/total*100,1)} for s in species],
            "conditions": [{"condition": c.get("condition"), "count": int(c["n"])} for c in conditions]
        }
        print(f"✓ tree_inventory: {total} trees, crepe myrtle {crepe_pct}%")
    except Exception as e:
        errors.append(f"tree_inventory: {e}")
        print(f"✗ tree_inventory: {e}")

    # ── 4. MEETING NOTICES (dszu-h9cf) ────────────────────────────────────
    try:
        wetlands = query("dszu-h9cf", {
            "$where": "upper(meeting_body) like '%WETLAND%'",
            "$order": "meeting_date DESC",
            "$limit": "10",
            "$select": "meeting_body,meeting_date,meeting_location,meeting_status"
        })
        trees_mtg = query("dszu-h9cf", {
            "$where": "upper(meeting_body) like '%TREE%'",
            "$order": "meeting_date DESC",
            "$limit": "10",
            "$select": "meeting_body,meeting_date,meeting_location,meeting_status"
        })
        out["datasets"]["meeting_notices"] = {
            "dataset_id": "dszu-h9cf",
            "label": "Public Meeting Notices — Wetlands Board & Tree Commission",
            "wetlands_board": wetlands,
            "tree_commission": trees_mtg,
            "wetlands_board_count": len(wetlands),
            "tree_commission_count": len(trees_mtg),
            "note": "Zero meetings may indicate bodies not convening or not reporting publicly"
        }
        print(f"✓ meeting_notices: {len(wetlands)} wetlands, {len(trees_mtg)} tree commission")
    except Exception as e:
        errors.append(f"meeting_notices: {e}")
        print(f"✗ meeting_notices: {e}")

    # ── 5. TIDE SENSORS (mgyn-4sni) ───────────────────────────────────────
    try:
        latest = query("mgyn-4sni", {"$order": "localtime DESC", "$limit": "1"})
        out["datasets"]["tide_sensors"] = {
            "dataset_id": "mgyn-4sni",
            "label": "Tide Sensors — Latest Readings (ft NAVD88)",
            "latest": latest[0] if latest else {}
        }
        print(f"✓ tide_sensors: {latest[0].get('localtime','?') if latest else 'no data'}")
    except Exception as e:
        errors.append(f"tide_sensors: {e}")
        print(f"✗ tide_sensors: {e}")

    out["errors"] = errors
    out["success"] = len(errors) == 0

    os.makedirs("data", exist_ok=True)
    with open("data/gip_snapshot.json", "w") as f:
        json.dump(out, f, indent=2)
    print(f"\nWrote data/gip_snapshot.json — {len(errors)} errors")
    return 0 if not errors else 1

if __name__ == "__main__":
    sys.exit(run())

#!/usr/bin/env python3
"""
Generate detailed per-member spending data for drill-down views.
Uses bulk queries for performance on the 1.5GB database.
"""

import json
import os
import re
import sqlite3
from collections import defaultdict

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "house_disbursements.db")
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "website", "data")


def normalize_fiscal_year(fy):
    if not fy:
        return ""
    m = re.search(r"(\d{4})", fy)
    return m.group(1) if m else fy


def main():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    print("Generating member detail data (bulk queries)...")

    # Bulk query: all MRA spending by office, year, category
    print("  Loading all MRA spending...")
    c.execute("""
        SELECT organization, fiscal_year, category, SUM(amount) as total, COUNT(*) as txns
        FROM disbursements
        WHERE record_type = 'DETAIL'
        AND (program LIKE '%REPRESENTATIONAL%' OR program LIKE '%MEMBER%')
        AND category != '' AND organization != ''
        GROUP BY organization, fiscal_year, category
    """)

    # Build nested structure: office -> year -> category -> {total, txns}
    office_data = defaultdict(lambda: defaultdict(dict))
    for row in c.fetchall():
        org, fy, cat, total, txns = row
        year = normalize_fiscal_year(fy)
        if not year or not (2016 <= int(year) <= 2025):
            continue
        office_data[org][year][cat] = {"total": round(total, 2), "transactions": txns}

    print(f"  Found {len(office_data)} offices")

    # Bulk query: top vendors per office
    print("  Loading vendor data...")
    c.execute("""
        SELECT organization, vendor_name, SUM(amount) as total, COUNT(*) as txns
        FROM disbursements
        WHERE record_type = 'DETAIL'
        AND (program LIKE '%REPRESENTATIONAL%' OR program LIKE '%MEMBER%')
        AND vendor_name != '' AND organization != ''
        GROUP BY organization, vendor_name
    """)

    office_vendors = defaultdict(list)
    for row in c.fetchall():
        org, vendor, total, txns = row
        office_vendors[org].append({"name": vendor, "total": round(total, 2), "transactions": txns})

    # Sort and keep top 15 per office
    for org in office_vendors:
        office_vendors[org].sort(key=lambda x: -x["total"])
        office_vendors[org] = office_vendors[org][:15]

    # Compute chamber averages
    print("  Computing chamber averages...")
    chamber_avgs = defaultdict(lambda: defaultdict(lambda: {"total": 0.0, "offices": set()}))
    for org, years in office_data.items():
        for year, cats in years.items():
            for cat, data in cats.items():
                chamber_avgs[year][cat]["total"] += data["total"]
                chamber_avgs[year][cat]["offices"].add(org)

    chamber_avgs_out = {}
    for year in sorted(chamber_avgs.keys()):
        chamber_avgs_out[year] = {}
        for cat, data in chamber_avgs[year].items():
            num = len(data["offices"])
            chamber_avgs_out[year][cat] = {
                "total": round(data["total"], 2),
                "avg": round(data["total"] / max(num, 1), 2),
                "num_offices": num,
            }

    with open(os.path.join(DATA_DIR, "chamber_averages.json"), "w") as f:
        json.dump(chamber_avgs_out, f, indent=2)

    # Build member details
    print("  Building member details...")
    member_details = []
    for org, years in office_data.items():
        years_sorted = sorted(years.keys())
        yearly_totals = {y: round(sum(d["total"] for d in years[y].values()), 2) for y in years_sorted}
        total_all = sum(yearly_totals.values())

        # Year-over-year changes
        yoy = {}
        for i in range(1, len(years_sorted)):
            prev = yearly_totals[years_sorted[i-1]]
            curr = yearly_totals[years_sorted[i]]
            if prev > 0:
                yoy[years_sorted[i]] = round((curr - prev) / prev * 100, 1)

        member_details.append({
            "office": org,
            "total_spending": round(total_all, 2),
            "years_active": years_sorted,
            "yearly_totals": yearly_totals,
            "yearly_categories": {y: years[y] for y in years_sorted},
            "yoy_changes": yoy,
            "top_vendors": office_vendors.get(org, []),
        })

    member_details.sort(key=lambda x: -x["total_spending"])

    with open(os.path.join(DATA_DIR, "member_details.json"), "w") as f:
        json.dump(member_details[:500], f)
    print(f"  Saved {min(len(member_details), 500)} office details")

    # Franked mail analysis
    print("  Generating franked mail analysis...")
    franked_data = defaultdict(lambda: defaultdict(float))
    for org, years in office_data.items():
        for year, cats in years.items():
            for cat, data in cats.items():
                if "FRANKED" in cat.upper():
                    franked_data[org][year] += data["total"]

    franked_list = []
    for org, years in franked_data.items():
        total = sum(years.values())
        franked_list.append({
            "office": org,
            "total": round(total, 2),
            "by_year": {y: round(v, 2) for y, v in sorted(years.items())},
        })
    franked_list.sort(key=lambda x: -x["total"])

    with open(os.path.join(DATA_DIR, "franked_mail.json"), "w") as f:
        json.dump(franked_list[:200], f, indent=2)

    conn.close()
    print("Done!")


if __name__ == "__main__":
    main()

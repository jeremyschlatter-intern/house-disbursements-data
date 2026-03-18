#!/usr/bin/env python3
"""
Generate all data files needed for the website from the SQLite database.
Creates JSON files for the interactive explorer and CSV exports for download.
"""

import json
import os
import re
import sqlite3
import csv

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "house_disbursements.db")
WEBSITE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "website")
DATA_DIR = os.path.join(WEBSITE_DIR, "data")
EXPORT_DIR = os.path.join(WEBSITE_DIR, "exports")


def normalize_fiscal_year(fy):
    """Normalize fiscal year strings like 'FY2023', 'LY2023', '2023' to just '2023'."""
    if not fy:
        return ""
    m = re.search(r"(\d{4})", fy)
    return m.group(1) if m else fy


def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(EXPORT_DIR, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    print("Generating website data files...")

    # 1. Overall summary stats
    print("  - summary.json")
    c.execute("SELECT COUNT(*) FROM disbursements WHERE record_type = 'DETAIL'")
    total_records = c.fetchone()[0]

    c.execute("SELECT SUM(amount) FROM disbursements WHERE record_type = 'DETAIL'")
    total_spending = c.fetchone()[0] or 0

    c.execute("SELECT COUNT(DISTINCT organization) FROM disbursements WHERE record_type = 'DETAIL'")
    total_orgs = c.fetchone()[0]

    c.execute("SELECT COUNT(DISTINCT vendor_name) FROM disbursements WHERE record_type = 'DETAIL' AND vendor_name != ''")
    total_vendors = c.fetchone()[0]

    summary = {
        "total_records": total_records,
        "total_spending": round(total_spending, 2),
        "total_organizations": total_orgs,
        "total_vendors": total_vendors,
        "quarters_covered": "Q3 2009 - Q4 2025",
        "data_source": "U.S. House of Representatives Chief Administrative Officer",
        "last_updated": "2026-03-18",
    }
    with open(os.path.join(DATA_DIR, "summary.json"), "w") as f:
        json.dump(summary, f, indent=2)

    # 2. Annual spending breakdown
    print("  - annual_spending.json")
    c.execute("""
        SELECT fiscal_year,
               SUM(amount) as total,
               COUNT(*) as num_transactions
        FROM disbursements
        WHERE record_type = 'DETAIL'
        GROUP BY fiscal_year
        ORDER BY fiscal_year
    """)
    raw_annual = {}
    for row in c.fetchall():
        year = normalize_fiscal_year(row[0])
        if year and 2005 <= int(year) <= 2026:
            if year not in raw_annual:
                raw_annual[year] = {"total": 0, "transactions": 0}
            raw_annual[year]["total"] += row[1]
            raw_annual[year]["transactions"] += row[2]

    annual_spending = [
        {"year": int(y), "total": round(d["total"], 2), "transactions": d["transactions"]}
        for y, d in sorted(raw_annual.items())
    ]
    with open(os.path.join(DATA_DIR, "annual_spending.json"), "w") as f:
        json.dump(annual_spending, f, indent=2)

    # 3. Spending by category per year
    print("  - category_spending.json")
    c.execute("""
        SELECT fiscal_year, category, SUM(amount) as total
        FROM disbursements
        WHERE record_type = 'DETAIL' AND category != ''
        GROUP BY fiscal_year, category
        ORDER BY fiscal_year, total DESC
    """)
    category_data = {}
    for row in c.fetchall():
        year = normalize_fiscal_year(row[0])
        if year and 2005 <= int(year) <= 2026:
            if year not in category_data:
                category_data[year] = {}
            cat = row[1]
            if cat not in category_data[year]:
                category_data[year][cat] = 0
            category_data[year][cat] += row[2]

    # Round values
    for year in category_data:
        for cat in category_data[year]:
            category_data[year][cat] = round(category_data[year][cat], 2)

    with open(os.path.join(DATA_DIR, "category_spending.json"), "w") as f:
        json.dump(category_data, f, indent=2)

    # 4. Quarterly spending breakdown
    print("  - quarterly_spending.json")
    c.execute("""
        SELECT quarter,
               SUM(amount) as total,
               COUNT(*) as num_transactions,
               SUM(CASE WHEN category = 'PERSONNEL COMPENSATION' THEN amount ELSE 0 END) as personnel,
               SUM(CASE WHEN category = 'TRAVEL' THEN amount ELSE 0 END) as travel,
               SUM(CASE WHEN category LIKE '%FRANKED%' THEN amount ELSE 0 END) as franked_mail
        FROM disbursements
        WHERE record_type = 'DETAIL'
        GROUP BY quarter
        ORDER BY quarter
    """)
    quarterly = [
        {
            "quarter": row[0],
            "total": round(row[1], 2),
            "transactions": row[2],
            "personnel": round(row[3], 2),
            "travel": round(row[4], 2),
            "franked_mail": round(row[5], 2),
        }
        for row in c.fetchall()
    ]
    with open(os.path.join(DATA_DIR, "quarterly_spending.json"), "w") as f:
        json.dump(quarterly, f, indent=2)

    # 5. Top offices (member representational spending)
    print("  - member_offices.json")
    c.execute("""
        SELECT organization,
               fiscal_year,
               SUM(amount) as total,
               COUNT(*) as num_transactions
        FROM disbursements
        WHERE record_type = 'DETAIL'
        AND (program LIKE '%REPRESENTATIONAL%' OR program LIKE '%MEMBER%')
        AND organization != ''
        GROUP BY organization, fiscal_year
        ORDER BY organization, fiscal_year
    """)
    member_data = {}
    for row in c.fetchall():
        org = row[0]
        year = normalize_fiscal_year(row[1])
        if not year:
            continue
        if org not in member_data:
            member_data[org] = {}
        if year not in member_data[org]:
            member_data[org][year] = {"total": 0, "transactions": 0}
        member_data[org][year]["total"] += row[2]
        member_data[org][year]["transactions"] += row[3]

    # Create a simplified list for the website
    member_list = []
    for org, years in member_data.items():
        total_all_years = sum(y["total"] for y in years.values())
        member_list.append({
            "office": org,
            "total_spending": round(total_all_years, 2),
            "years_active": sorted(years.keys()),
            "annual_spending": {y: round(d["total"], 2) for y, d in sorted(years.items())},
        })
    member_list.sort(key=lambda x: -x["total_spending"])

    with open(os.path.join(DATA_DIR, "member_offices.json"), "w") as f:
        json.dump(member_list[:500], f, indent=2)  # Top 500 offices

    # 6. Top vendors
    print("  - top_vendors.json")
    c.execute("""
        SELECT vendor_name,
               SUM(amount) as total,
               COUNT(*) as num_transactions,
               MIN(transaction_date) as first_seen,
               MAX(transaction_date) as last_seen
        FROM disbursements
        WHERE record_type = 'DETAIL' AND vendor_name != ''
        GROUP BY vendor_name
        ORDER BY total DESC
        LIMIT 200
    """)
    vendors = [
        {
            "name": row[0],
            "total": round(row[1], 2),
            "transactions": row[2],
            "first_seen": row[3],
            "last_seen": row[4],
        }
        for row in c.fetchall()
    ]
    with open(os.path.join(DATA_DIR, "top_vendors.json"), "w") as f:
        json.dump(vendors, f, indent=2)

    # 7. Category breakdown (overall)
    print("  - categories.json")
    c.execute("""
        SELECT category,
               SUM(amount) as total,
               COUNT(*) as num_transactions
        FROM disbursements
        WHERE record_type = 'DETAIL' AND category != ''
        GROUP BY category
        ORDER BY total DESC
    """)
    categories = [
        {"category": row[0], "total": round(row[1], 2), "transactions": row[2]}
        for row in c.fetchall()
    ]
    with open(os.path.join(DATA_DIR, "categories.json"), "w") as f:
        json.dump(categories, f, indent=2)

    # 8. Generate downloadable CSV exports per year
    print("  - CSV exports by year")
    c.execute("SELECT DISTINCT fiscal_year FROM disbursements WHERE record_type = 'DETAIL'")
    all_years = set()
    for row in c.fetchall():
        year = normalize_fiscal_year(row[0])
        if year and 2005 <= int(year) <= 2026:
            all_years.add(year)

    export_info = []
    for year in sorted(all_years):
        print(f"    - {year}")
        c.execute("""
            SELECT organization, fiscal_year, program, category,
                   transaction_date, vendor_name, start_date, end_date,
                   description, amount, document
            FROM disbursements
            WHERE record_type = 'DETAIL' AND fiscal_year LIKE ?
            ORDER BY transaction_date, organization
        """, (f"%{year}%",))

        rows = c.fetchall()
        if not rows:
            continue

        filename = f"house_disbursements_{year}.csv"
        filepath = os.path.join(EXPORT_DIR, filename)
        with open(filepath, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "Organization", "Fiscal Year", "Program", "Category",
                "Transaction Date", "Vendor Name", "Start Date", "End Date",
                "Description", "Amount", "Document"
            ])
            for row in rows:
                writer.writerow(row)

        size_mb = os.path.getsize(filepath) / (1024 * 1024)
        export_info.append({
            "year": int(year),
            "filename": filename,
            "records": len(rows),
            "size_mb": round(size_mb, 1),
        })

    with open(os.path.join(DATA_DIR, "exports.json"), "w") as f:
        json.dump(export_info, f, indent=2)

    # 9. Generate a master export CSV
    print("  - Master export CSV")
    c.execute("""
        SELECT quarter, organization, fiscal_year, program, category,
               transaction_date, vendor_name, start_date, end_date,
               description, amount, document, data_source
        FROM disbursements
        WHERE record_type = 'DETAIL'
        ORDER BY quarter, transaction_date, organization
    """)

    master_path = os.path.join(EXPORT_DIR, "house_disbursements_all_2016_2025.csv")
    with open(master_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Quarter", "Organization", "Fiscal Year", "Program", "Category",
            "Transaction Date", "Vendor Name", "Start Date", "End Date",
            "Description", "Amount", "Document", "Data Source"
        ])
        batch_size = 50000
        while True:
            rows = c.fetchmany(batch_size)
            if not rows:
                break
            for row in rows:
                writer.writerow(row)

    master_size = os.path.getsize(master_path) / (1024 * 1024)
    print(f"    Master CSV: {master_size:.1f} MB")

    conn.close()
    print("\nDone! Website data generated.")


if __name__ == "__main__":
    main()

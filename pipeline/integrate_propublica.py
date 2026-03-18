#!/usr/bin/env python3
"""
Integrate ProPublica-parsed CSV data (2009-2015) into the unified SQLite database.

The ProPublica format has different columns than the official House CSVs:
OFFICE, QUARTER, CATEGORY, DATE, PAYEE, START DATE, END DATE, PURPOSE, AMOUNT,
YEAR, TRANSCODE, TRANSCODELONG, RECORDID, RECIP (orig.)
"""

import csv
import os
import re
import sqlite3
from datetime import datetime

PARSED_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "propublica_parsed")
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "house_disbursements.db")


def parse_propublica_date(date_str, year):
    """Parse ProPublica date formats (MM-DD with year context, or MM/DD/YY)."""
    if not date_str or not date_str.strip():
        return ""
    date_str = date_str.strip()
    # MM/DD/YY format
    if "/" in date_str:
        try:
            return datetime.strptime(date_str, "%m/%d/%y").strftime("%Y-%m-%d")
        except ValueError:
            return date_str
    # MM-DD format (need year)
    if "-" in date_str and len(date_str) <= 5:
        try:
            return datetime.strptime(f"{date_str}-{year}", "%m-%d-%Y").strftime("%Y-%m-%d")
        except ValueError:
            return date_str
    return date_str


def parse_amount(s):
    if not s or not s.strip():
        return 0.0
    s = s.strip().replace(",", "").replace("$", "").replace('"', '')
    if s.startswith("(") and s.endswith(")"):
        s = "-" + s[1:-1]
    try:
        return float(s)
    except ValueError:
        return 0.0


def main():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Check if propublica data already exists
    c.execute("SELECT COUNT(*) FROM disbursements WHERE data_source = 'PROPUBLICA'")
    existing = c.fetchone()[0]
    if existing > 0:
        print(f"Removing {existing} existing ProPublica records...")
        c.execute("DELETE FROM disbursements WHERE data_source = 'PROPUBLICA'")
        conn.commit()

    detail_files = sorted([f for f in os.listdir(PARSED_DIR) if f.endswith("-detail.csv")])
    print(f"Found {len(detail_files)} ProPublica detail CSVs")

    total_inserted = 0
    for filename in detail_files:
        # Extract quarter from filename like "2013Q4-house-disburse-detail.csv"
        quarter = filename[:6]  # "2013Q4"
        year = filename[:4]
        filepath = os.path.join(PARSED_DIR, filename)

        records = []
        with open(filepath, "r", encoding="utf-8-sig", errors="replace") as f:
            reader = csv.DictReader(f)
            for row in reader:
                office = row.get("OFFICE", "").strip().strip('"')
                category = row.get("CATEGORY", "").strip().strip('"')
                amount = parse_amount(row.get("AMOUNT", ""))
                payee = row.get("PAYEE", "").strip().strip('"')
                purpose = row.get("PURPOSE", "").strip().strip('"')
                date_str = row.get("DATE", "").strip().strip('"')
                start_date = row.get("START DATE", "").strip().strip('"')
                end_date = row.get("END DATE", "").strip().strip('"')
                transcode = row.get("TRANSCODE", "").strip().strip('"')
                record_id = row.get("RECORDID", "").strip().strip('"')
                row_year = row.get("YEAR", year).strip().strip('"')

                records.append((
                    quarter,                              # quarter
                    office,                               # organization
                    row_year,                             # fiscal_year
                    "MEMBERS REPRESENTATIONAL ALLOWANCE",  # program (ProPublica only parsed MRA)
                    category,                             # category
                    "DETAIL",                             # record_type
                    parse_propublica_date(date_str, row_year),  # transaction_date
                    "PROPUBLICA",                         # data_source (to distinguish from official CSVs)
                    record_id,                            # document
                    payee,                                # vendor_name
                    "",                                   # vendor_id
                    parse_propublica_date(start_date, row_year),  # start_date
                    parse_propublica_date(end_date, row_year),    # end_date
                    purpose,                              # description
                    amount,                               # amount
                    "",                                   # organization_code
                    "",                                   # program_code
                    "",                                   # budget_object_class
                    "",                                   # budget_object_code
                ))

        c.executemany("""
            INSERT INTO disbursements (
                quarter, organization, fiscal_year, program, category,
                record_type, transaction_date, data_source, document,
                vendor_name, vendor_id, start_date, end_date, description,
                amount, organization_code, program_code,
                budget_object_class, budget_object_code
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, records)

        total_inserted += len(records)
        print(f"  {quarter}: {len(records):,} records")

    conn.commit()

    # Show new totals
    c.execute("SELECT COUNT(*) FROM disbursements WHERE record_type = 'DETAIL'")
    total_detail = c.fetchone()[0]
    c.execute("SELECT SUM(amount) FROM disbursements WHERE record_type = 'DETAIL'")
    total_spending = c.fetchone()[0]
    c.execute("SELECT MIN(quarter), MAX(quarter) FROM disbursements")
    q_range = c.fetchone()

    print(f"\nInserted {total_inserted:,} ProPublica records")
    print(f"Database now spans: {q_range[0]} to {q_range[1]}")
    print(f"Total detail records: {total_detail:,}")
    print(f"Total spending: ${total_spending:,.2f}")

    conn.close()


if __name__ == "__main__":
    main()

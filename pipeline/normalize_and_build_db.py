#!/usr/bin/env python3
"""
Normalize all House disbursement CSVs into a unified schema and build a SQLite database.

Handles the format differences between:
- 2016-2022 (12-column format)
- 2022Q4+ (18-column format with organization codes, vendor IDs, etc.)
"""

import csv
import os
import re
import sqlite3
import json
from datetime import datetime

RAW_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "house_csvs_raw")
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "house_disbursements.db")
STATS_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "stats.json")

# Unified schema for all records
UNIFIED_COLUMNS = [
    "quarter",           # e.g., "2016Q1"
    "organization",      # Office name
    "fiscal_year",       # Extracted from ORGANIZATION or FISCAL YEAR field
    "program",           # e.g., "MEMBERS REPRESENTATIONAL ALLOWANCE"
    "category",          # SORT SUBTOTAL DESCRIPTION - e.g., "PERSONNEL COMPENSATION"
    "record_type",       # DETAIL, SUBTOTAL, or GRAND TOTAL
    "transaction_date",  # Normalized to YYYY-MM-DD
    "data_source",       # AP, GL, etc.
    "document",          # Document number
    "vendor_name",       # Payee/vendor
    "vendor_id",         # New field from 2022Q4+
    "start_date",        # Period start, normalized to YYYY-MM-DD
    "end_date",          # Period end, normalized to YYYY-MM-DD
    "description",       # Purpose/description
    "amount",            # Dollar amount as float
    "organization_code", # New field from 2022Q4+
    "program_code",      # New field from 2022Q4+
    "budget_object_class",
    "budget_object_code",
]


def parse_date(date_str):
    """Parse various date formats to YYYY-MM-DD."""
    if not date_str or not date_str.strip():
        return ""
    date_str = date_str.strip()
    for fmt in ["%d-%b-%y", "%d-%b-%Y", "%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d"]:
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return date_str  # Return as-is if no format matches


def parse_amount(amount_str):
    """Parse dollar amount string to float."""
    if not amount_str or not amount_str.strip():
        return 0.0
    s = amount_str.strip().replace(",", "").replace("$", "")
    # Handle parenthesized negatives: (1234.56)
    if s.startswith("(") and s.endswith(")"):
        s = "-" + s[1:-1]
    try:
        return float(s)
    except ValueError:
        return 0.0


def extract_fiscal_year(org_str):
    """Extract fiscal year from organization string like '2016 OFFICE OF THE SPEAKER'."""
    m = re.match(r"^(\d{4})\s+", org_str)
    return m.group(1) if m else ""


def clean_organization(org_str):
    """Remove leading year from organization name."""
    return re.sub(r"^\d{4}\s+", "", org_str).strip()


def parse_quarter_from_filename(filename):
    """Extract quarter from filename like '2016Q1_detail.csv'."""
    m = re.match(r"(\d{4}Q\d)", filename)
    return m.group(1) if m else ""


def process_12col_row(row, quarter):
    """Process a row from the 12-column format (2016-2022)."""
    # Columns: ORGANIZATION, PROGRAM, SORT SUBTOTAL DESCRIPTION, SORT SEQUENCE,
    # TRANSACTION DATE, DATA SOURCE, DOCUMENT, VENDOR NAME,
    # PERFORM START DT, PERFORM END DT, DESCRIPTION, AMOUNT
    org_raw = row.get("ORGANIZATION", "").strip()
    return {
        "quarter": quarter,
        "organization": clean_organization(org_raw),
        "fiscal_year": extract_fiscal_year(org_raw),
        "program": row.get("PROGRAM", "").strip(),
        "category": row.get("SORT SUBTOTAL DESCRIPTION", "").strip(),
        "record_type": row.get("SORT SEQUENCE", "").strip(),
        "transaction_date": parse_date(row.get("TRANSACTION DATE", "")),
        "data_source": row.get("DATA SOURCE", "").strip(),
        "document": row.get("DOCUMENT", "").strip(),
        "vendor_name": row.get("VENDOR NAME", "").strip(),
        "vendor_id": "",
        "start_date": parse_date(row.get("PERFORM START DT", "")),
        "end_date": parse_date(row.get("PERFORM END DT", "")),
        "description": row.get("DESCRIPTION", "").strip(),
        "amount": parse_amount(row.get("AMOUNT", "")),
        "organization_code": "",
        "program_code": "",
        "budget_object_class": "",
        "budget_object_code": "",
    }


def process_18col_row(row, quarter):
    """Process a row from the 18-column format (2022Q4+)."""
    org_raw = row.get("ORGANIZATION", "").strip()
    return {
        "quarter": quarter,
        "organization": clean_organization(org_raw),
        "fiscal_year": row.get("FISCAL YEAR OR LEGISLATIVE YEAR", extract_fiscal_year(org_raw)).strip(),
        "program": row.get("PROGRAM", "").strip(),
        "category": row.get("SORT SUBTOTAL DESCRIPTION", "").strip(),
        "record_type": row.get("SORT SEQUENCE", "").strip(),
        "transaction_date": parse_date(row.get("TRANSACTION DATE", "")),
        "data_source": row.get("DATA SOURCE", "").strip(),
        "document": row.get("DOCUMENT", "").strip(),
        "vendor_name": row.get("VENDOR NAME", "").strip(),
        "vendor_id": row.get("VENDOR ID", "").strip(),
        "start_date": parse_date(row.get("PERFORM START DT", "")),
        "end_date": parse_date(row.get("PERFORM END DT", "")),
        "description": row.get("DESCRIPTION", "").strip(),
        "amount": parse_amount(row.get("AMOUNT", "")),
        "organization_code": row.get("ORGANIZATION CODE", "").strip(),
        "program_code": row.get("PROGRAM CODE", "").strip(),
        "budget_object_class": row.get("BUDGET OBJECT CLASS", "").strip(),
        "budget_object_code": row.get("BUDGET OBJECT CODE", "").strip(),
    }


def detect_format(headers):
    """Detect whether this is 12-col or 18-col format."""
    clean_headers = [h.strip() for h in headers if h.strip()]
    if "FISCAL YEAR OR LEGISLATIVE YEAR" in clean_headers or "ORGANIZATION CODE" in clean_headers:
        return 18
    return 12


def process_csv(filepath, quarter):
    """Process a single CSV file and yield normalized records."""
    records = []
    with open(filepath, "r", encoding="utf-8-sig", errors="replace") as f:
        reader = csv.DictReader(f)
        # Strip whitespace from header names (AMOUNT has trailing spaces in some files)
        if reader.fieldnames:
            reader.fieldnames = [h.strip() for h in reader.fieldnames]
        headers = reader.fieldnames or []
        fmt = detect_format(headers)

        for row in reader:
            # Strip keys in each row to match cleaned headers
            row = {k.strip(): v for k, v in row.items() if k}
            try:
                if fmt == 18:
                    record = process_18col_row(row, quarter)
                else:
                    record = process_12col_row(row, quarter)

                # Skip empty rows
                if not record["organization"] and not record["vendor_name"] and record["amount"] == 0.0:
                    continue

                records.append(record)
            except Exception as e:
                continue  # Skip malformed rows

    return records


def create_database(all_records):
    """Create SQLite database from normalized records."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("DROP TABLE IF EXISTS disbursements")
    c.execute("""
        CREATE TABLE disbursements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            quarter TEXT,
            organization TEXT,
            fiscal_year TEXT,
            program TEXT,
            category TEXT,
            record_type TEXT,
            transaction_date TEXT,
            data_source TEXT,
            document TEXT,
            vendor_name TEXT,
            vendor_id TEXT,
            start_date TEXT,
            end_date TEXT,
            description TEXT,
            amount REAL,
            organization_code TEXT,
            program_code TEXT,
            budget_object_class TEXT,
            budget_object_code TEXT
        )
    """)

    # Insert records
    cols = UNIFIED_COLUMNS
    placeholders = ",".join(["?"] * len(cols))
    insert_sql = f"INSERT INTO disbursements ({','.join(cols)}) VALUES ({placeholders})"

    batch = []
    for record in all_records:
        values = tuple(record[col] for col in cols)
        batch.append(values)
        if len(batch) >= 10000:
            c.executemany(insert_sql, batch)
            batch = []

    if batch:
        c.executemany(insert_sql, batch)

    # Create indices
    c.execute("CREATE INDEX idx_quarter ON disbursements(quarter)")
    c.execute("CREATE INDEX idx_organization ON disbursements(organization)")
    c.execute("CREATE INDEX idx_fiscal_year ON disbursements(fiscal_year)")
    c.execute("CREATE INDEX idx_category ON disbursements(category)")
    c.execute("CREATE INDEX idx_vendor_name ON disbursements(vendor_name)")
    c.execute("CREATE INDEX idx_record_type ON disbursements(record_type)")
    c.execute("CREATE INDEX idx_amount ON disbursements(amount)")

    conn.commit()
    return conn


def generate_stats(conn):
    """Generate aggregate statistics for the website."""
    c = conn.cursor()

    stats = {}

    # Total records
    c.execute("SELECT COUNT(*) FROM disbursements WHERE record_type = 'DETAIL'")
    stats["total_detail_records"] = c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM disbursements")
    stats["total_records"] = c.fetchone()[0]

    # Date range
    c.execute("SELECT MIN(quarter), MAX(quarter) FROM disbursements")
    row = c.fetchone()
    stats["earliest_quarter"] = row[0]
    stats["latest_quarter"] = row[1]

    # Total spending (detail records only)
    c.execute("SELECT SUM(amount) FROM disbursements WHERE record_type = 'DETAIL'")
    stats["total_spending"] = round(c.fetchone()[0] or 0, 2)

    # Spending by year
    c.execute("""
        SELECT fiscal_year, SUM(amount) as total
        FROM disbursements
        WHERE record_type = 'DETAIL' AND fiscal_year != ''
        GROUP BY fiscal_year
        ORDER BY fiscal_year
    """)
    stats["spending_by_year"] = {row[0]: round(row[1], 2) for row in c.fetchall()}

    # Spending by category
    c.execute("""
        SELECT category, SUM(amount) as total
        FROM disbursements
        WHERE record_type = 'DETAIL' AND category != ''
        GROUP BY category
        ORDER BY total DESC
        LIMIT 20
    """)
    stats["spending_by_category"] = {row[0]: round(row[1], 2) for row in c.fetchall()}

    # Top organizations by spending
    c.execute("""
        SELECT organization, SUM(amount) as total
        FROM disbursements
        WHERE record_type = 'DETAIL' AND organization != ''
        GROUP BY organization
        ORDER BY total DESC
        LIMIT 50
    """)
    stats["top_organizations"] = {row[0]: round(row[1], 2) for row in c.fetchall()}

    # Top vendors
    c.execute("""
        SELECT vendor_name, SUM(amount) as total, COUNT(*) as num_transactions
        FROM disbursements
        WHERE record_type = 'DETAIL' AND vendor_name != ''
        GROUP BY vendor_name
        ORDER BY total DESC
        LIMIT 50
    """)
    stats["top_vendors"] = [
        {"name": row[0], "total": round(row[1], 2), "transactions": row[2]}
        for row in c.fetchall()
    ]

    # Quarters available
    c.execute("SELECT DISTINCT quarter FROM disbursements ORDER BY quarter")
    stats["quarters"] = [row[0] for row in c.fetchall()]

    # Programs
    c.execute("""
        SELECT program, SUM(amount) as total
        FROM disbursements
        WHERE record_type = 'DETAIL' AND program != ''
        GROUP BY program
        ORDER BY total DESC
    """)
    stats["spending_by_program"] = {row[0]: round(row[1], 2) for row in c.fetchall()}

    # Record count by quarter
    c.execute("""
        SELECT quarter, COUNT(*) as cnt, SUM(CASE WHEN record_type='DETAIL' THEN amount ELSE 0 END) as total
        FROM disbursements
        GROUP BY quarter
        ORDER BY quarter
    """)
    stats["by_quarter"] = [
        {"quarter": row[0], "records": row[1], "total_spending": round(row[2], 2)}
        for row in c.fetchall()
    ]

    # Member offices spending (offices that contain "HON." or are in MRA program)
    c.execute("""
        SELECT organization, fiscal_year, SUM(amount) as total
        FROM disbursements
        WHERE record_type = 'DETAIL'
        AND program LIKE '%REPRESENTATIONAL%'
        GROUP BY organization, fiscal_year
        ORDER BY total DESC
        LIMIT 100
    """)
    stats["member_spending"] = [
        {"office": row[0], "year": row[1], "total": round(row[2], 2)}
        for row in c.fetchall()
    ]

    return stats


def main():
    print("Normalizing House disbursement CSVs...")

    detail_files = sorted([
        f for f in os.listdir(RAW_DIR)
        if f.endswith("_detail.csv")
    ])

    all_records = []
    for filename in detail_files:
        quarter = parse_quarter_from_filename(filename)
        filepath = os.path.join(RAW_DIR, filename)
        records = process_csv(filepath, quarter)
        all_records.extend(records)
        print(f"  {quarter}: {len(records):,} records")

    print(f"\nTotal records: {len(all_records):,}")

    print("\nBuilding SQLite database...")
    conn = create_database(all_records)
    db_size = os.path.getsize(DB_PATH) / (1024 * 1024)
    print(f"Database created: {DB_PATH} ({db_size:.1f} MB)")

    print("\nGenerating statistics...")
    stats = generate_stats(conn)

    with open(STATS_PATH, "w") as f:
        json.dump(stats, f, indent=2)
    print(f"Statistics saved: {STATS_PATH}")

    # Print summary
    print(f"\n{'='*60}")
    print(f"SUMMARY")
    print(f"{'='*60}")
    print(f"Quarters covered: {stats['earliest_quarter']} to {stats['latest_quarter']}")
    print(f"Total detail records: {stats['total_detail_records']:,}")
    print(f"Total spending tracked: ${stats['total_spending']:,.2f}")
    print(f"Number of quarters: {len(stats['quarters'])}")

    conn.close()


if __name__ == "__main__":
    main()

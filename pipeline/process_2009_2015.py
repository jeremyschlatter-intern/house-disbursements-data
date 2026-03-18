#!/usr/bin/env python3
"""
Download and process 2009-2015 House Statement of Disbursements PDFs
using the ProPublica parser.

Pipeline: PDF -> pdftotext -layout -> ProPublica parse-disbursements.py -> CSV
"""

import os
import sys
import subprocess
import urllib.request
import time

BASE_URL = "https://www.house.gov/sites/default/files/uploads/documents/"

# Map of (year, quarter) -> PDF filename on house.gov
# 2009Q3 uses a hyphen; 2009Q4 uses uppercase Q and underscore; rest use lowercase q and underscore
PDF_URLS = {}

# 2009Q3 special case - hyphen
PDF_URLS[(2009, 3)] = "2009q3-singlevolume.pdf"
# 2009Q4 special case - uppercase Q
PDF_URLS[(2009, 4)] = "2009Q4_singlevolume.pdf"

# 2010-2015: lowercase q, underscore
for year in range(2010, 2016):
    for q in range(1, 5):
        PDF_URLS[(year, q)] = f"{year}q{q}_singlevolume.pdf"


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PARSER_PATH = os.path.join(PROJECT_ROOT, "propublica-disbursements", "1_pdf_to_csv", "parse-disbursements.py")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "data", "propublica_parsed")
DOWNLOAD_DIR = os.path.join(PROJECT_ROOT, "data", "pdfs_2009_2015")
TEXT_DIR = os.path.join(PROJECT_ROOT, "data", "text_2009_2015")


def download_pdf(year, quarter, pdf_filename):
    """Download a PDF from house.gov if not already present."""
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    local_path = os.path.join(DOWNLOAD_DIR, pdf_filename)

    if os.path.exists(local_path):
        size_mb = os.path.getsize(local_path) / (1024 * 1024)
        print(f"  Already downloaded: {pdf_filename} ({size_mb:.1f} MB)")
        return local_path

    url = BASE_URL + pdf_filename
    print(f"  Downloading {url} ...")
    start = time.time()

    try:
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0 (research project - processing public House disbursement data)'
        })
        with urllib.request.urlopen(req, timeout=300) as response:
            data = response.read()
        with open(local_path, 'wb') as f:
            f.write(data)
        elapsed = time.time() - start
        size_mb = len(data) / (1024 * 1024)
        print(f"  Downloaded {size_mb:.1f} MB in {elapsed:.0f}s")
        return local_path
    except Exception as e:
        print(f"  ERROR downloading {url}: {e}")
        if os.path.exists(local_path):
            os.remove(local_path)
        return None


def extract_text(pdf_path, year, quarter):
    """Run pdftotext -layout on a PDF."""
    os.makedirs(TEXT_DIR, exist_ok=True)
    # Name the text file so the parser can extract year/quarter from filename
    # Parser expects filename[:4] = year, filename[4:6] = quarter (e.g. "2013Q4")
    txt_filename = f"{year}Q{quarter}-disbursements.txt"
    txt_path = os.path.join(TEXT_DIR, txt_filename)

    if os.path.exists(txt_path) and os.path.getsize(txt_path) > 0:
        size_mb = os.path.getsize(txt_path) / (1024 * 1024)
        print(f"  Text already extracted: {txt_filename} ({size_mb:.1f} MB)")
        return txt_path

    print(f"  Extracting text with pdftotext -layout ...")
    start = time.time()
    try:
        result = subprocess.run(
            ["pdftotext", "-layout", pdf_path, txt_path],
            capture_output=True, text=True, timeout=600
        )
        if result.returncode != 0:
            print(f"  ERROR pdftotext: {result.stderr}")
            return None
        elapsed = time.time() - start
        size_mb = os.path.getsize(txt_path) / (1024 * 1024)
        print(f"  Extracted {size_mb:.1f} MB text in {elapsed:.0f}s")
        return txt_path
    except subprocess.TimeoutExpired:
        print(f"  ERROR: pdftotext timed out after 600s")
        return None
    except Exception as e:
        print(f"  ERROR: {e}")
        return None


def run_parser(txt_path, year, quarter):
    """Run the ProPublica parser on the text file."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # The parser writes output files to the current working directory
    # based on year+quarter from the filename
    quarter_str = f"Q{quarter}"
    detail_csv = f"{year}{quarter_str}-house-disburse-detail.csv"
    summary_csv = f"{year}{quarter_str}-house-disburse-summary.csv"
    trash_file = f"{year}{quarter_str}-trashlines.txt"

    detail_path = os.path.join(OUTPUT_DIR, detail_csv)
    summary_path = os.path.join(OUTPUT_DIR, summary_csv)

    if os.path.exists(detail_path) and os.path.getsize(detail_path) > 100:
        print(f"  Parser output already exists: {detail_csv}")
        return True

    print(f"  Running ProPublica parser ...")
    start = time.time()
    try:
        result = subprocess.run(
            [sys.executable, PARSER_PATH, txt_path],
            capture_output=True, text=True, timeout=600,
            cwd=OUTPUT_DIR  # parser writes to cwd
        )
        elapsed = time.time() - start
        if result.returncode != 0:
            print(f"  ERROR parser: {result.stderr}")
            return False
        if result.stderr:
            print(f"  Parser stderr: {result.stderr[:200]}")

        # Check output
        if os.path.exists(detail_path):
            size_kb = os.path.getsize(detail_path) / 1024
            print(f"  Parser complete in {elapsed:.0f}s - detail CSV: {size_kb:.0f} KB")
            return True
        else:
            print(f"  ERROR: Parser did not produce expected output file {detail_csv}")
            # Check what files were created
            created = [f for f in os.listdir(OUTPUT_DIR) if f.startswith(str(year))]
            if created:
                print(f"  Files created: {created}")
            return False
    except subprocess.TimeoutExpired:
        print(f"  ERROR: Parser timed out after 600s")
        return False
    except Exception as e:
        print(f"  ERROR: {e}")
        return False


def main():
    print("=" * 70)
    print("Processing 2009-2015 House Statements of Disbursements")
    print(f"Parser: {PARSER_PATH}")
    print(f"Output: {OUTPUT_DIR}")
    print("=" * 70)

    results = {}

    for (year, quarter) in sorted(PDF_URLS.keys()):
        pdf_filename = PDF_URLS[(year, quarter)]
        label = f"{year}Q{quarter}"
        print(f"\n{'='*50}")
        print(f"Processing {label} ({pdf_filename})")
        print(f"{'='*50}")

        # Step 1: Download
        pdf_path = download_pdf(year, quarter, pdf_filename)
        if not pdf_path:
            results[label] = "FAILED (download)"
            continue

        # Step 2: Extract text
        txt_path = extract_text(pdf_path, year, quarter)
        if not txt_path:
            results[label] = "FAILED (pdftotext)"
            continue

        # Step 3: Parse
        success = run_parser(txt_path, year, quarter)
        if success:
            results[label] = "OK"
        else:
            results[label] = "FAILED (parser)"

    # Summary
    print(f"\n{'='*70}")
    print("SUMMARY")
    print(f"{'='*70}")
    ok_count = sum(1 for v in results.values() if v == "OK")
    total = len(results)
    print(f"{ok_count}/{total} quarters processed successfully\n")
    for label in sorted(results.keys()):
        status = results[label]
        marker = "OK" if status == "OK" else "FAIL"
        print(f"  [{marker:4s}] {label}: {status}")

    # List output files
    print(f"\nOutput files in {OUTPUT_DIR}:")
    if os.path.exists(OUTPUT_DIR):
        for f in sorted(os.listdir(OUTPUT_DIR)):
            fpath = os.path.join(OUTPUT_DIR, f)
            size = os.path.getsize(fpath)
            if size > 1024*1024:
                print(f"  {f} ({size/(1024*1024):.1f} MB)")
            elif size > 1024:
                print(f"  {f} ({size/1024:.0f} KB)")
            else:
                print(f"  {f} ({size} B)")


if __name__ == "__main__":
    main()

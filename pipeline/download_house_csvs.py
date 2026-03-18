#!/usr/bin/env python3
"""Download all available House Statement of Disbursements CSVs from house.gov (2016-2025)."""

import os
import time
import urllib.request
import urllib.parse

BASE_URL = "https://www.house.gov"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "house_csvs_raw")

# All known CSV download paths from house.gov archive, organized by quarter
CSV_URLS = {
    # 2025
    "2025Q4": {
        "detail": "/sites/default/files/2026-02/OCT-DEC-2025-SOD-DETAIL-GRID-FINAL.csv",
        "summary": "/sites/default/files/2026-02/OCT-DEC-2025-SOD-SUMMARY-GRID-FINAL.csv",
    },
    "2025Q3": {
        "detail": "/sites/default/files/2025-11/grids/JULY-SEPTEMBER%202025%20SOD%20DETAIL%20GRID-FINAL.csv",
        "summary": "/sites/default/files/2025-11/grids/JULY-SEPTEMBER%202025%20SOD%20SUMMARY%20GRID-FINAL.csv",
    },
    "2025Q2": {
        "detail": "/sites/default/files/2025-08/APRIL-JUNE%202025%20SOD%20DETAIL%20GRID-FINAL.csv",
        "summary": "/sites/default/files/2025-08/APRIL-JUNE%202025%20SOD%20SUMMARY%20GRID-FINAL.csv",
    },
    "2025Q1": {
        "detail": "/sites/default/files/2025-05/JANUARY-MARCH-2025-SOD-DETAIL-GRID-FINAL.csv",
        "summary": "/sites/default/files/2025-05/JANUARY-MARCH-2025-SOD-SUMMARY-GRID-FINAL.csv",
    },
    # 2024
    "2024Q4": {
        "detail": "/sites/default/files/2025-02/OCTOBER-DECEMBER-2024-SOD-DETAIL-GRID-FINAL.csv",
        "summary": "/sites/default/files/2025-02/OCTOBER-DECEMBER-2024-SOD-SUMMARY-GRID-FINAL.csv",
    },
    "2024Q3": {
        "detail": "/sites/default/files/2024-11/JULY-SEPTEMBER_2024_SOD_DETAIL_GRID-FINAL.csv",
        "summary": "/sites/default/files/2024-11/JULY-SEPTEMBER_2024_SOD_SUMMARY_GRID-FINAL.csv",
    },
    "2024Q2": {
        "detail": "/sites/default/files/2024-08/APRIL-JUNE-2024-SOD-DETAIL-GRID-FINAL.csv",
        "summary": "/sites/default/files/2024-08/APRIL-JUNE-2024-SOD-SUMMARY-GRID-FINAL.csv",
    },
    "2024Q1": {
        "detail": "/sites/default/files/2024-05/JAN-MAR-2024-SOD-DETAIL-GRID-FINAL.csv",
        "summary": "/sites/default/files/2024-05/JAN-MAR-2024-SOD-SUMMARY-GRID-FINAL.csv",
    },
    # 2023
    "2023Q4": {
        "detail": "/sites/default/files/2024-02/OCT-DEC-2023-SOD-DETAIL-GRID-FINAL.csv",
        "summary": "/sites/default/files/2024-02/OCT-DEC-2023-SOD-SUMMARY-GRID-FINAL.csv",
    },
    "2023Q3": {
        "detail": "/sites/default/files/2023-11/JULY-SEPTEMBER-2023-SOD-DETAIL-GRID-FINAL.csv",
        "summary": "/sites/default/files/2023-11/JULY-SEPTEMBER-SOD-SUMMARY-GRID-FINAL.csv",
    },
    "2023Q2": {
        "detail": "/sites/default/files/2023-08/APRIL-JUNE%202023%20SOD%20DETAIL%20GRID-FINAL.csv",
        "summary": "/sites/default/files/2023-08/APRIL-JUNE-2023-SOD-SUMMARY-GRID-FINAL.csv",
    },
    "2023Q1": {
        "detail": "/sites/default/files/2023-05/JAN-MAR-2023-SOD-DETAIL-GRID-FINAL.csv",
        "summary": "/sites/default/files/2023-05/JAN-MAR-2023-SOD-SUMMARY-GRID-FINAL.csv",
    },
    # 2022
    "2022Q4": {
        "detail": "/sites/default/files/2023-02/OCT-DEC-2022-SOD-DETAIL-GRID-FINAL.csv",
        "summary": "/sites/default/files/2023-02/OCT-DEC-2022-SOD-SUMMARY-GRID-FINAL.csv",
    },
    "2022Q3": {
        "detail": "/sites/default/files/2022-11/JULY-SEPT-2022-SOD-DETAIL-GRID-FINAL.csv",
        "summary": "/sites/default/files/2022-11/JULY-SEPT-2022-SOD-SUMMARY-GRID-FINAL.csv",
    },
    "2022Q2": {
        "detail": "/sites/default/files/2022-08/APR-JUNE-2022-SOD-DETAIL-GRID-FINAL.csv",
        "summary": "/sites/default/files/2022-08/APR-JUNE-2022-SOD-SUMMARY-GRID-FINAL.csv",
    },
    "2022Q1": {
        "detail": "/sites/default/files/2022-05/JAN-MAR-2022-SOD-DETAIL-GRID-FINAL.csv",
        "summary": "/sites/default/files/2022-05/JAN-MAR-2022-SOD-SUMM-GRID-FINAL.csv",
    },
    # 2021
    "2021Q4": {
        "detail": "/sites/default/files/uploads/documents/SODs/2021q4/OCT-DEC-2021-SOD-DETAIL-GRID-FINAL.csv",
        "summary": "/sites/default/files/uploads/documents/SODs/2021q4/OCT-DEC-2021-SOD-SUMM-GRID-FINAL.csv",
    },
    "2021Q3": {
        "detail": "/sites/default/files/uploads/documents/SODs/2021q3/JULY-2021-SOD-DETAIL-GRID-FINAL.csv",
        "summary": "/sites/default/files/uploads/documents/SODs/2021q3/JULY-SEPT-2021-SOD-SUMM-GRID-FINAL.csv",
    },
    "2021Q2": {
        "detail": "/sites/default/files/uploads/documents/SODs/2021q2/APR-JUN%202021%20SOD%20DETAIL%20GRID_FINAL.csv",
        "summary": "/sites/default/files/uploads/documents/SODs/2021q2/APR-JUN%202021%20SOD%20SUMM%20GRID_FINAL.csv",
    },
    "2021Q1": {
        "detail": "/sites/default/files/uploads/documents/SODs/2021q1/JAN_MAR_2021_SOD_DETAIL_GRID_FINAL.csv",
        "summary": "/sites/default/files/uploads/documents/SODs/2021q1/JAN_MAR_2021_SOD_SUMM_GRID_FINAL.csv",
    },
    # 2020
    "2020Q4": {
        "detail": "/sites/default/files/uploads/documents/SODs/2020q4/OCT-DEC%202020%20SOD%20DETAIL%20GRID_FINAL.csv",
        "summary": "/sites/default/files/uploads/documents/SODs/2020q4/OCT-DEC%202020%20SOD%20SUMM%20GRID_FINAL.csv",
    },
    "2020Q3": {
        "detail": "/sites/default/files/uploads/documents/SODs/2020q3/JULY-SEPT-2020-SOD-DETAIL-GRID-FINAL.csv",
        "summary": "/sites/default/files/uploads/documents/SODs/2020q3/JULY-SEPT-2020-SOD-SUMM-GRID-FINAL.csv",
    },
    "2020Q2": {
        "detail": "/sites/default/files/uploads/documents/SODs/APR-JUN-2020-SOD-DETAIL-GRID_FINAL.csv",
        "summary": "/sites/default/files/uploads/documents/SODs/APR-JUN-2020-SOD-SUMMARY-GRID_FINAL.csv",
    },
    "2020Q1": {
        "detail": "/sites/default/files/uploads/documents/SODs/JAN-MAR-2020-SOD-DETAIL-GRID_FINAL.csv",
        "summary": "/sites/default/files/uploads/documents/SODs/JAN-MAR-2020-SOD-SUMMARY-GRID_FINAL.csv",
    },
    # 2019
    "2019Q4": {
        "detail": "/sites/default/files/uploads/documents/SODs/OCT-DEC-2019-SOD-DETAIL-GRID.csv",
        "summary": "/sites/default/files/uploads/documents/SODs/OCT-DEC-2019-SOD-SUMMARY-GRID.csv",
    },
    "2019Q3": {
        "detail": "/sites/default/files/uploads/documents/SODs/JUL-SEPT%202019%20SOD%20DETAIL%20GRID.csv",
        "summary": "/sites/default/files/uploads/documents/SODs/JUL-SEPT%202019%20SOD%20SUMMARY%20GRID.csv",
    },
    "2019Q2": {
        "detail": "/sites/default/files/uploads/documents/SODs/APR-JUN%202019%20SOD%20DETAIL%20GRID.csv",
        "summary": "/sites/default/files/uploads/documents/SODs/APR-JUN%202019%20SOD%20SUMMARY%20GRID.csv",
    },
    "2019Q1": {
        "detail": "/sites/default/files/uploads/documents/SODs/JAN-MAR%202019%20SOD%20DETAIL%20GRID.CSV",
        "summary": "/sites/default/files/uploads/documents/SODs/JAN-MAR%202019%20SOD%20SUMMARY%20GRID.CSV",
    },
    # 2018
    "2018Q4": {
        "detail": "/sites/default/files/uploads/documents/SODs/OCT-DEC%202018%20SOD%20DETAIL%20GRID.csv",
        "summary": "/sites/default/files/uploads/documents/SODs/OCT-DEC%202018%20SOD%20SUMMARY%20GRID.csv",
    },
    "2018Q3": {
        "detail": "/sites/default/files/uploads/documents/SODs/JULY-SEPTEMBER%202018%20SOD%20DETAIL%20GRID.csv",
        "summary": "/sites/default/files/uploads/documents/SODs/JULY-SEPTEMBER%202018%20SOD%20SUMMARY%20GRID.csv",
    },
    "2018Q2": {
        "detail": "/sites/default/files/uploads/documents/SODs/APR-JUNE-2018-SOD-DETAIL-GRID.csv",
        "summary": "/sites/default/files/uploads/documents/SODs/APR-JUNE-2018-SOD-SUMMARY-GRID.csv",
    },
    "2018Q1": {
        "detail": "/sites/default/files/uploads/documents/JAN-MAR%202018%20SOD%20DETAIL%20GRID.csv",
        "summary": "/sites/default/files/uploads/documents/JAN-MAR%202018%20SOD%20SUMMARY%20GRID.csv",
    },
    # 2017
    "2017Q4": {
        "detail": "/sites/default/files/uploads/documents/SODs/OCT-DEC%202017%20SOD%20DETAIL%20GRID.csv",
        "summary": "/sites/default/files/uploads/documents/SODs/OCT-DEC%202017%20SOD%20SUMMARY%20GRID.csv",
    },
    "2017Q3": {
        "detail": "/sites/default/files/uploads/documents/SODs/JUL-SEPT%202017%20SOD%20DETAIL%20GRID.csv",
        "summary": "/sites/default/files/uploads/documents/SODs/JUL-SEPT%202017%20SOD%20SUMMARY%20GRID.csv",
    },
    "2017Q2": {
        "detail": "/sites/default/files/uploads/documents/APR-JUN%202017%20DETAIL%20GRID.csv",
        "summary": "/sites/default/files/uploads/documents/APR-JUN%202017%20SUMMARY%20GRID.csv",
    },
    "2017Q1": {
        "detail": "/sites/default/files/uploads/documents/SODs/JAN-MAR%202017%20DETAIL%20GRID.csv",
        "summary": "/sites/default/files/uploads/documents/SODs/JAN-MAR%202017%20SUMM%20GRID.csv",
    },
    # 2016
    "2016Q4": {
        "detail": "/sites/default/files/uploads/documents/OCT-DEC%202016%20DETAIL%20GRID.csv",
        "summary": "/sites/default/files/uploads/documents/OCT-DEC%202016%20SUMM%20GRID.csv",
    },
    "2016Q3": {
        "detail": "/sites/default/files/uploads/documents/JULY-SEPT-2016-SOD-DETAIL-GRID.csv",
        "summary": "/sites/default/files/uploads/documents/JULY-SEPT-2016-SOD-SUMM-GRID.csv",
    },
    "2016Q2": {
        "detail": "/sites/default/files/uploads/documents/APR-JUNE-2016-SOD-DETAIL-GRID-REVISE-9_26_16.csv",
        "summary": "/sites/default/files/uploads/documents/APR-JUNE-2016-SOD-SUMM-GRID.CSV",
    },
    "2016Q1": {
        "detail": "/sites/default/files/uploads/documents/JAN-MAR-2016-SOD-DETAIL-GRID_REVISED_9_26_16.csv",
        "summary": "/sites/default/files/uploads/documents/JAN-MAR-2016-SOD-SUMM-GRID.csv",
    },
}


def download_file(url, filepath):
    """Download a file from a URL to a local path."""
    if os.path.exists(filepath):
        print(f"  Already exists: {os.path.basename(filepath)}")
        return True
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "HouseDisbursements/1.0"})
        with urllib.request.urlopen(req, timeout=60) as response:
            with open(filepath, "wb") as f:
                f.write(response.read())
        size_mb = os.path.getsize(filepath) / (1024 * 1024)
        print(f"  Downloaded: {os.path.basename(filepath)} ({size_mb:.1f} MB)")
        return True
    except Exception as e:
        print(f"  FAILED: {os.path.basename(filepath)} - {e}")
        return False


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    total = len(CSV_URLS) * 2
    success = 0
    failed = 0

    for quarter in sorted(CSV_URLS.keys()):
        urls = CSV_URLS[quarter]
        print(f"\n{quarter}:")

        for file_type in ["detail", "summary"]:
            path = urls[file_type]
            url = BASE_URL + path
            filename = f"{quarter}_{file_type}.csv"
            filepath = os.path.join(OUTPUT_DIR, filename)

            if download_file(url, filepath):
                success += 1
            else:
                failed += 1

            time.sleep(0.5)  # Be polite to the server

    print(f"\n\nDone! {success}/{total} files downloaded, {failed} failed.")
    print(f"Files saved to: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()

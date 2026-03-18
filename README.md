# House Disbursements Data Explorer

**Live site:** [jeremyschlatter-intern.github.io/house-disbursements-data](https://jeremyschlatter-intern.github.io/house-disbursements-data/)

An interactive explorer and unified dataset of U.S. House of Representatives spending data, covering 66 quarters from Q3 2009 through Q4 2025.

## What's in the dataset

- **7 million+** individual disbursement records
- **$23.2 billion** in tracked spending
- **2,400+** organizations (member offices, committees, leadership, administrative)
- **338,000+** unique vendors/payees
- **16 years** of quarterly data

## Data sources

| Period | Source | Method |
|--------|--------|--------|
| 2016-2025 | Official CSV files from [house.gov](https://www.house.gov/the-house-explained/open-government/statement-of-disbursements) | Downloaded and normalized |
| 2009-2015 | Born-digital PDFs from house.gov | Text extracted with `pdftotext`, parsed with [ProPublica parser](https://github.com/propublica/disbursements) |

## Website features

- Annual and quarterly spending trend charts
- Searchable member office browser with drill-down views
- Chamber average comparisons for individual offices
- Top vendor analysis
- Spending category breakdowns
- Franked mail analysis
- Downloadable CSV exports by year

## Data pipeline

The `pipeline/` directory contains Python scripts that:

1. **`download_house_csvs.py`** - Downloads all 80 official CSVs from house.gov (2016-2025)
2. **`process_2009_2015.py`** - Downloads PDFs, extracts text, runs ProPublica parser (2009-2015)
3. **`normalize_and_build_db.py`** - Normalizes all formats into a unified SQLite database
4. **`integrate_propublica.py`** - Integrates ProPublica-parsed data into the database
5. **`generate_website_data.py`** - Generates JSON data files for the website
6. **`generate_member_details.py`** - Generates per-office detail data with chamber averages

## Downloads

Compressed CSV exports are available from the [GitHub releases page](https://github.com/jeremyschlatter-intern/house-disbursements-data/releases).

## License

All source data is published by the U.S. Government and is in the public domain. The processing code in this repository is also released to the public domain.

# After Action Report: Statements of Disbursements as Data

## Project Summary

**Goal:** Convert the U.S. House of Representatives' Statements of Disbursements into structured data and publish on a website.

**Result:** A publicly deployed interactive data explorer covering 16 years of House spending (2009-2025), with 7 million+ records and $23.2 billion in tracked disbursements. Downloadable CSV exports are available for researchers.

**Live site:** https://jeremyschlatter-intern.github.io/house-disbursements-data/
**Source code:** https://github.com/jeremyschlatter-intern/house-disbursements-data
**Data downloads:** https://github.com/jeremyschlatter-intern/house-disbursements-data/releases/tag/v1.0.0

---

## Process

### Phase 1: Research (30 min)

I started by researching the landscape of existing tools and data:

- **Sunlight Foundation / ProPublica parser** (GitHub): A Python regex parser that extracts structured data from pdftotext output of House disbursement PDFs. Originally by Luke Rosiak, improved by James Turk.
- **house.gov archive**: Official CSVs available from Q1 2016 onward; PDFs from Q3 2009 onward.
- **Boston Public Library / Internet Archive**: Scanned PDFs of historical reports (1970-2008), digitized with OCR.
- **Prior projects**: Data4Democracy, NewsAppsUMD, and others who worked with this data.

**Key finding:** The gap that remained unfilled was a unified, downloadable dataset that spanned the maximum available date range and was easily accessible to researchers.

### Phase 2: Data Assessment - The OCR Problem (20 min)

I downloaded sample OCR text files from the Internet Archive for reports from 1980, 1990, 1996, and 2005. This revealed a major obstacle:

**Obstacle: The OCR quality of pre-2009 scanned PDFs is extremely poor.** The Internet Archive's Tesseract-based OCR produced text that was largely garbled - leader dots, column alignments, and special characters were misread. Dollar amounts and names were recognizable in isolation, but the structured table format was destroyed.

**What I tried:**
- Examined djvu.txt files from multiple decades
- Searched for recognizable patterns (dates, dollar amounts, office names)
- Evaluated whether regex-based parsing could work

**Decision:** Focusing on the pre-2009 scanned data would require either re-OCR'ing from page images with better tools, or AI-powered interpretation of page images. Neither was feasible at the necessary scale within this session. I pivoted to maximizing coverage from the existing high-quality sources.

### Phase 3: Official CSV Pipeline (40 min)

I built a pipeline to download and normalize all 80 official House CSV files (2016Q1-2025Q4).

**Obstacle: Inconsistent CSV formats.** The House changed its CSV format multiple times:
- 2016-2022: 12 columns
- Some quarters in 2020-2022: 13 columns (trailing comma)
- 2022Q4+: 18 columns (added organization codes, vendor IDs, budget object codes)

**Obstacle: Trailing whitespace in column headers.** The AMOUNT column header had dozens of trailing spaces, causing the CSV parser to silently return empty values. The first database build showed $0.00 total spending. I discovered the issue by inspecting the raw DictReader keys and added header stripping.

**Result:** Successfully downloaded and normalized all 80 CSVs into a unified SQLite database. 4.6 million records, $15.15 billion in spending.

### Phase 4: ProPublica Parser for 2009-2015 (45 min)

I cloned the ProPublica disbursements repository and studied the parser's regex-based approach. I then built a pipeline to:

1. Download all 26 quarterly PDFs from house.gov (2009Q3-2015Q4)
2. Extract text using `pdftotext -layout`
3. Run the ProPublica parser on each text file
4. Integrate the output into the unified database

This was run as a background agent while I worked on the website.

**Obstacle: Large PDF files.** The PDFs ranged from 30-70MB each. Downloading 26 of them and running pdftotext took significant time. I handled this by running it as a background process while building the website in parallel.

**Result:** Added 2.75 million additional records, extending coverage back to Q3 2009. The dataset now spans 66 quarters with 7 million+ records.

### Phase 5: Interactive Website (60 min)

I built a static HTML/JavaScript website with Chart.js for visualizations:

- Overview dashboard with key statistics and charts
- Annual and quarterly spending trends
- Stacked category breakdown (personnel vs non-personnel)
- Searchable member office browser
- Member detail drill-down views with chamber average comparisons
- Top vendor table
- Dedicated franked mail analysis
- Download page with links to GitHub releases
- About page documenting methodology and sources

**Obstacle: Member detail data generation was slow.** My initial approach ran individual SQL queries for each of 1,000+ offices against a 1.5GB database. Each query took ~5 seconds, making the whole process take hours. I rewrote it to use two bulk queries (one for all spending by office/year/category, one for all vendor spending by office), then processed the results in memory. This reduced runtime to ~30 seconds.

### Phase 6: Deployment and Publishing (20 min)

I created a GitHub repository, set up GitHub Actions for Pages deployment, and created a release with compressed CSV exports.

**Obstacle: Large files in git.** The initial push included PDFs and text extracts totaling several GB. I cleaned these out of git tracking, added them to .gitignore, and instead made the data available via GitHub releases (compressed from ~1.2GB to 125MB for the master CSV).

### Phase 7: Feedback and Iteration (30 min)

I created a simulated DC stakeholder agent (playing Daniel Schuman of Demand Progress) to evaluate the solution. Key feedback and responses:

| Feedback | Action |
|----------|--------|
| Process 2009-2015 data | Done - added 2.75M records |
| Add member drill-down views | Done - category breakdowns, vendor lists, chamber comparisons |
| Deploy publicly | Done - GitHub Pages |
| Add franked mail analysis | Done - dedicated section with trend charts |
| Fix generic GitHub link | Done |
| Vendor deduplication | Not done (would require significant entity resolution work) |
| Inflation adjustment | Not done (would need CPI data integration) |
| Staff-level tracking | Not done (complex entity resolution across years) |

---

## Team Members

| Role | Description |
|------|-------------|
| **Main agent** (me) | Architecture, website development, data pipeline, integration |
| **Research agent** | Investigated existing tools, data sources, and prior work |
| **PDF processor agent** | Downloaded and parsed 2009-2015 PDFs (ran in background) |
| **DC stakeholder agent** | Evaluated the solution from a transparency community perspective |
| **Codebase explorer agent** | Analyzed the ProPublica parser code in detail |

---

## What Was Delivered

1. **Unified dataset** spanning 2009-2025 (7M+ records, $23.2B)
2. **Interactive website** with search, filtering, drill-downs, and visualizations
3. **Downloadable CSV exports** by year and as a complete dataset
4. **Reproducible pipeline** - Python scripts that can re-process all data from scratch
5. **Public GitHub repository** with source code and documentation

## What Remains

1. **Pre-2009 historical data** - Requires better OCR or AI-powered extraction from scanned PDFs
2. **Vendor deduplication** - Many vendors appear under slight name variations
3. **Staff-level tracking** - Individual names in personnel records could enable workforce analysis
4. **Senate data** - Semi-annual Secretary of the Senate reports (PDF-only)
5. **Inflation adjustment** - Would make longitudinal comparisons more meaningful

## Key Metrics

| Metric | Value |
|--------|-------|
| Quarters covered | 66 (Q3 2009 - Q4 2025) |
| Total records | 7,035,634 |
| Detail transactions | 7,035,634 |
| Total spending tracked | $23,244,414,466 |
| Unique organizations | 2,406 |
| Unique vendors | 338,526 |
| Per-year CSV exports | 17 files |
| Master CSV size | 1.2 GB (125 MB compressed) |
| Website JSON data | 3.5 MB |

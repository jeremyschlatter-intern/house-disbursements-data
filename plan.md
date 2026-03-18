# Statements of Disbursements as Data - Implementation Plan

## Problem
The House's Statements of Disbursements from 1970-2009 exist only as scanned PDFs on the Internet Archive. They have OCR text layers but have never been parsed into structured data. Meanwhile, 2009-2015 data was parsed by Sunlight Foundation/ProPublica, and 2016+ is published as CSV by the House itself.

## Goal
Build a pipeline to extract structured data from the historical scanned PDFs (1970-2009), unifying it with the existing parsed data (2009-2016) and official House CSVs (2016+) to create a comprehensive, decades-spanning dataset of House spending. Publish it on a website.

## Approach

### Phase 1: Understand the Data
- Download sample PDFs from Internet Archive for different eras (1980s, 1990s, 2000s)
- Examine the OCR text quality and report format changes over time
- Study the ProPublica parser to understand the expected format

### Phase 2: Build the Parsing Pipeline
- Adapt/extend the ProPublica regex-based parser for historical formats
- Handle format variations across decades (pre-1995 "Report of the Clerk" vs post-1995 "Statement of Disbursements")
- Extract: Office, Date, Payee, Purpose, Amount, Category
- Output standardized CSV matching a unified schema

### Phase 3: Process Historical Data
- Build an Internet Archive downloader to systematically fetch reports
- Process a meaningful subset (e.g., 1980-2009) through the pipeline
- Validate output quality

### Phase 4: Unify the Dataset
- Merge historical parsed data with ProPublica-era data and official House CSVs
- Create a unified schema with consistent column names
- Add metadata (congress number, session, fiscal year)

### Phase 5: Build the Website
- Static site with search/browse/download capabilities
- Data explorer with filtering by year, office, category
- Downloadable CSV/JSON files by quarter
- Documentation of methodology and data quality

### Phase 6: Polish & Iterate
- Get feedback from DC agent (playing Daniel Schuman role)
- Fix issues, improve data quality, enhance website
- Write after-action report

## Key Technical Decisions
- Python for parsing (extending ProPublica's approach)
- Use Internet Archive's text files (already OCR'd) rather than re-OCR'ing
- Static website (no server needed) - probably using a simple HTML/JS approach
- Focus on demonstrating the pipeline works on historical data, even if we can't process ALL decades in one session

## Success Criteria
- Working pipeline that extracts structured data from historical PDFs
- Unified dataset spanning multiple decades
- Publishable website with search and download
- DC stakeholder (agent) satisfaction

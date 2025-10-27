# CIA Factbook Scraper

Autonomous scraper for the CIA World Factbook that extracts, stores, and tracks changes in country data.

## Overview

This tool scrapes the CIA World Factbook by fetching structured JSON data directly from Gatsby's static page-data.json files. It performs automatic field discovery, tracks historical changes, and analyzes data coverage across all countries.

## Features

- **No browser automation**: Fetches JSON directly from static files
- **Automatic field discovery**: Identifies all available data fields without manual configuration
- **Historical tracking**: Stores snapshots and detects changes over time
- **Coverage analysis**: Reports which countries have which data fields

## Project Structure
```
cia-factbook-scraper/
├── config/          # Configuration files
├── discovery/       # Sitemap parsing and structure analysis
├── scrapers/        # Data fetching and extraction
├── analyzers/       # Coverage and change detection
├── data/
│   ├── index/       # Countries list and field catalog
│   ├── snapshots/   # Historical data snapshots
│   └── diffs/       # Change detection results
├── utils/           # Helper functions
└── logs/            # Execution logs
```

## Installation
```bash
pip install -r requirements.txt
```

## Usage

Coming soon - CLI interface in development

## Data Source

CIA World Factbook: https://www.cia.gov/the-world-factbook/
Public domain data.

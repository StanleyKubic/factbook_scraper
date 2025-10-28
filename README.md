# CIA Factbook Scraper

Autonomous scraper for the CIA World Factbook that extracts, stores, and tracks changes in country data through a modular, multi-stage processing pipeline.

## Overview

This tool systematically scrapes the CIA World Factbook by fetching structured JSON data directly from Gatsby's static page-data files. It performs automatic field discovery, tracks historical changes, and analyzes data coverage across all countries through a coordinated workflow of specialized modules.

## Execution Flow

The scraper operates through a sequential pipeline that transforms raw web data into structured, analyzed datasets:

```mermaid
graph TD
    A[Start: main.py] --> B[Discovery Phase]
    B --> C[Scraping Phase] 
    C --> D[Analysis Phase]
    D --> E[Refinement Phase]
    
    subgraph "Discovery Phase"
        B1[sitemap_parser.py] --> B1F[data/index/countries.json]
        B2[category_mapper.py] --> B2F[data/index/category_mapping.json]
        B1F --> B2F
        B2F --> B3F
    end
    
    subgraph "Scraping Phase"
        C1[Load Index Files] --> C2[Sequential Processing]
        C2 --> C3[fetcher.py: HTTP Request]
        C3 --> C4[parser.py: JSON Transform]
        C4 --> C5[Save Raw Files]
        B1F --> C1
        B2F --> C1
        C5 --> CF[data/snapshots/YYYY-MM-DD/raw/]
    end
    
    subgraph "Analysis Phase"
        D1[Load Raw Files] --> D2[field_discovery.py]
        CF --> D1
        D2 --> D3[Field Registry]
        D3 --> D4[Coverage Analysis]
        D4 --> DF[data/snapshots/YYYY-MM-DD/analysis/field_catalog.json]
    end
    
    subgraph "Refinement Phase"
        E1[Load Raw Files] --> E2[multi_value_splitter.py]
        CF --> E1
        E2 --> E3[Multi-Value Detection]
        E3 --> E4[Data Splitting & Normalization]
        E4 --> EF[data/snapshots/YYYY-MM-DD/refined/]
        E4 --> ER[data/snapshots/YYYY-MM-DD/analysis/multi_value_report.json]
    end
```

### Detailed File Interaction Flow

```mermaid
graph LR
    subgraph "Input Sources"
        WEB[CIA Factbook Website]
        CONFIG[config.yaml]
    end
    
    subgraph "Discovery Outputs"
        COUNTRIES[data/index/countries.json]
        CATEGORIES[data/index/category_mapping.json]
        STRUCTURE[data/index/structure_analysis.json]
    end
    
    subgraph "Processing Pipeline"
        FETCHER[scrapers/fetcher.py]
        PARSER[scrapers/parser.py]
        ANALYZER[analyzers/field_discovery.py]
        REFINER[refiners/multi_value_splitter.py]
    end
    
    subgraph "Snapshot Files"
        RAW[data/snapshots/DATE/raw/]
        REFINED[data/snapshots/DATE/refined/]
        ANALYSIS[data/snapshots/DATE/analysis/]
        REPORTS[data/snapshots/DATE/reports/]
    end
    
    WEB --> COUNTRIES
    WEB --> CATEGORIES
    CONFIG --> FETCHER
    CONFIG --> ANALYZER
    CONFIG --> REFINER
    
    COUNTRIES --> FETCHER
    CATEGORIES --> PARSER
    
    FETCHER --> PARSER
    PARSER --> RAW
    
    RAW --> ANALYZER
    ANALYZER --> ANALYSIS
    
    RAW --> REFINER
    REFINER --> REFINED
    REFINER --> ANALYSIS
    
    RAW --> REPORTS
```

### Data Enrichment Process

```mermaid
graph TD
    A[Raw page-data.json] --> B[parser.py]
    B --> C[Raw Country JSON]
    C --> D{Has Category Mapping?}
    D -->|Yes| E[Enrich with Categories]
    D -->|No| F[Skip Enrichment]
    E --> G[Enhanced Raw JSON]
    F --> G
    G --> H[field_discovery.py]
    H --> I[Field Catalog & Coverage Analysis]
    G --> J[multi_value_splitter.py]
    J --> K[Multi-Value Detection]
    K --> L{Contains br tags?}
    L -->|Yes| M[Split Values Array]
    L -->|No| N[Keep Single Value]
    M --> O[Normalized Refined JSON]
    N --> O
    O --> P[Multi-Value Analysis Report]
    
    subgraph "Enrichment Sources"
        Q[data/index/category_mapping.json]
        Q --> E
    end
    
    subgraph "Analysis Outputs"
        I --> R[analysis/field_catalog.json]
        P --> S[analysis/multi_value_report.json]
    end
    
    subgraph "Data Storage"
        O --> T[refined/country.json]
        G --> U[raw/country.json]
    end
```

## Module Usage by Phase

### 🔍 Discovery Phase (`discovery/`)

**Purpose**: Map the CIA Factbook structure and create master indexes

#### `sitemap_parser.py`
- **Role**: Discovers all available country URLs from the Factbook sitemap
- **Process**: 
  1. Fetches sitemap XML from CIA Factbook
  2. Extracts all country-related URLs
  3. Categorizes URLs (main, factsheet, images, flag, map)
  4. Transforms web URLs to page-data.json URLs
  5. Creates `data/index/countries.json` master index

#### `category_mapper.py`
- **Role**: Maps field database IDs to meaningful categories
- **Process**:
  1. Fetches category structure from Factbook
  2. Extracts database_id → category mappings
  3. Creates `data/index/category_mapping.json` for data enrichment


### 📥 Scraping Phase (`scrapers/` + `main.py`)

**Purpose**: Fetch and parse raw country data from the source

#### `main.py` (Orchestrator)
- **Role**: Coordinates the complete scraping workflow
- **Process**:
  1. Loads configuration and country index
  2. Creates dated snapshot directory structure
  3. Orchestrates sequential country processing
  4. Generates metadata and execution logs
  5. Prints progress and summary statistics

#### `fetcher.py`
- **Role**: Reliably downloads page-data.json files
- **Process**:
  1. HTTP requests with retry logic and rate limiting
  2. JSON structure validation
  3. Error classification and handling
  4. Connection pooling for efficiency

#### `parser.py`
- **Role**: Transforms verbose Gatsby JSON into clean structure
- **Process**:
  1. Extracts country metadata (name, region, updated date)
  2. Simplifies field structures while preserving raw data
  3. Extracts media assets (flags, maps, images)
  4. Validates and normalizes data formats

**Data Flow**:
```
Countries Index → Sequential Processing → HTTP Fetch → JSON Parse → Raw Files
     ↓                    ↓                    ↓           ↓            ↓
countries.json   →   country URLs    →   page-data.json → clean data → snapshot/YYYY-MM-DD/raw/
```

### 📊 Analysis Phase (`analyzers/`)

**Purpose**: Discover patterns and analyze data coverage across all countries

#### `field_discovery.py`
- **Role**: Analyzes field availability and coverage statistics
- **Process**:
  1. Loads all country JSON files from snapshot
  2. Builds comprehensive field registry
  3. Calculates coverage percentages by field
  4. Analyzes field distribution by category
  5. Creates `snapshot/YYYY-MM-DD/analysis/field_catalog.json`

**Analysis Output**:
- Total unique fields discovered
- Coverage statistics (universal, common, rare fields)
- Category-wise field distribution
- Field metadata and database ID mappings

### 🔧 Refinement Phase (`refiners/`)

**Purpose**: Normalize and enhance data structure for analysis

#### `multi_value_splitter.py`
- **Role**: Splits multi-valued fields containing `<br>` tags
- **Process**:
  1. Detects fields with multiple values via `<br>` tags
  2. Splits values while preserving order
  3. Creates uniform data structure with values arrays
  4. Generates analysis reports on multi-value patterns
  5. Saves to `snapshot/YYYY-MM-DD/refined/` and `analysis/multi_value_report.json`

**Data Transformation**:
```json
// Before (Raw)
{
  "name": "GDP",
  "data": "$82B (2023)<br>$80B (2022)<br>$75B (2021)"
}

// After (Refined)
{
  "name": "GDP",
  "is_multi_valued": true,
  "values": [
    {"value": "$82B (2023)", "order": 0},
    {"value": "$80B (2022)", "order": 1},
    {"value": "$75B (2021)", "order": 2}
  ]
}
```

### 🛠️ Utilities (`utils/`)

**Purpose**: Shared functionality across all modules

#### `config.py`
- **Role**: Centralized configuration management
- **Provides**: Scraping parameters, logging settings, snapshot options

#### `logger.py`
- **Role**: Consistent logging across all modules
- **Provides**: Structured logging with configurable levels and outputs

#### `http_client.py`
- **Role**: Robust HTTP client with retry logic
- **Provides**: Connection pooling, rate limiting, error handling

## Data Flow Summary

```
Discovery Phase:
┌─────────────────┐    ┌──────────────────┐
│ Sitemap Parser  │───▶│ Category Mapper  │
└─────────────────┘    └──────────────────┘
        │                       │          
        ▼                       ▼           
countries.json   category_mapping.json 
        │                       │ 
        └───────────────────────┘
                   │
                   ▼
        Main Orchestrator (main.py)

Scraping Phase:
┌─────────────────┐    ┌───────────────────┐    ┌─────────────────────┐
│    Fetcher      │───▶│      Parser       │───▶│   Raw Data Files    │
│  (HTTP Client)  │    │ (Structure Fix)   │    │   snapshot/raw/     │
└─────────────────┘    └───────────────────┘    └─────────────────────┘

Analysis Phase:
┌─────────────────┐    ┌───────────────────┐    ┌─────────────────────┐
│ Field Discovery │───▶│ Coverage Analysis │───▶│  Field Catalog      │
│ (Load & Scan)   │    │ (Statistics)      │    │  analysis/          │
└─────────────────┘    └───────────────────┘    └─────────────────────┘

Refinement Phase:
┌─────────────────┐    ┌───────────────────┐    ┌─────────────────────┐
│Multi-Value Split│───▶│ Data Normalization│───▶│  Refined Data       │
│  (Detection)    │    │ (Uniform Format)  │    │  snapshot/refined/  │
└─────────────────┘    └───────────────────┘    └─────────────────────┘
```

## Project Structure

```
cia-factbook-scraper/
├── main.py                 # Main orchestrator and entry point
├── config/
│   └── config.yaml         # Central configuration file
├── discovery/              # Site structure discovery modules
│   ├── sitemap_parser.py   # URL discovery and categorization
│   └── category_mapper.py  # Field-to-category mapping
├── scrapers/               # Data fetching and parsing
│   ├── fetcher.py          # HTTP client with retry logic
│   └── parser.py           # JSON structure transformation
├── analyzers/              # Data analysis and discovery
│   └── field_discovery.py  # Field coverage and catalog generation
├── refiners/               # Data enhancement and normalization
│   └── multi_value_splitter.py # Multi-value field processing
├── utils/                  # Shared utilities
│   ├── config.py           # Configuration management
│   ├── logger.py           # Logging setup
│   └── http_client.py      # HTTP client base class
├── data/
│   ├── index/              # Generated indexes and mappings
│   │   ├── countries.json          # Master country index
│   │   ├── category_mapping.json   # Field category mappings
│   │   └── structure_analysis.json # Site structure analysis
│   └── snapshots/           # Time-based data snapshots
│       └── YYYY-MM-DD/
│           ├── raw/         # Original parsed data
│           ├── refined/     # Processed and normalized data
│           ├── reports/     # Execution logs and metadata
│           └── analysis/    # Analysis results
├── logs/                    # Application logs
└── requirements.txt         # Python dependencies
```

## Installation

```bash
pip install -r requirements.txt
```

## Usage

### Complete Workflow (Recommended)
```bash
# Run complete scraping pipeline
python main.py

# Scrape specific countries
python main.py --countries france germany japan

# Test run without saving files
python main.py --dry-run

# Override snapshot date
python main.py --date 2025-10-26
```

### Individual Module Usage
```bash
# Discovery phase
python -m discovery.sitemap_parser
python -m discovery.category_mapper

# Analysis phase
python -m analyzers.field_discovery --snapshot 2025-10-28

# Refinement phase
python -m refiners.multi_value_splitter --snapshot 2025-10-28
```

## Features

- **No browser automation**: Fetches JSON directly from static files
- **Automatic field discovery**: Identifies all available data fields without manual configuration
- **Historical tracking**: Stores snapshots and detects changes over time
- **Coverage analysis**: Reports which countries have which data fields
- **Multi-value handling**: Intelligently splits complex fields with multiple values
- **Category enrichment**: Maps technical database IDs to meaningful categories
- **Comprehensive logging**: Detailed execution tracking and error reporting

## Data Source

CIA World Factbook: https://www.cia.gov/the-world-factbook/
Public domain data.

## Output Files

### Index Files (`data/index/`)
- `countries.json`: Master list of all countries with URLs
- `category_mapping.json`: Field database ID to category mappings
- `structure_analysis.json`: Site structure and field patterns

### Snapshot Files (`data/snapshots/YYYY-MM-DD/`)
- `raw/`: Original parsed country data
- `refined/`: Processed data with normalized structure
- `reports/`: Execution logs, metadata, and statistics
- `analysis/`: Field catalogs and coverage analysis

## Key Insights

- **Sequential Processing**: Countries are processed one at a time to respect rate limits
- **Atomic Operations**: All file writes use temporary files and atomic renames
- **Error Recovery**: Comprehensive retry logic and error classification
- **Data Integrity**: Structure validation at every processing stage
- **Scalability**: Modular design allows for easy extension and modification

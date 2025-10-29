# CIA Factbook Scraper

Autonomous scraper for CIA World Factbook that extracts, stores, and tracks changes in country data through a modular, multi-stage processing pipeline.

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
    
    subgraph "Data Storage"
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
    CATEGORIES --> FETCHER
    
    FETCHER --> PARSER
    PARSER --> RAW
    PARSER --> ANALYZER
    PARSER --> REFINER
    
    RAW --> ANALYZER
    RAW --> REFINER
    
    ANALYZER --> ANALYSIS
    REFINER --> REFINED
    REFINER --> ANALYSIS
    
    RAW --> REPORTS
    ANALYSIS --> REPORTS
    REFINED --> REPORTS
```

## Module Usage by Phase

### 🔍 Discovery Phase (`discovery/`)

**Purpose**: Map CIA Factbook structure and create master indexes

#### `sitemap_parser.py`
- **Role**: Discovers all available country URLs from Factbook sitemap
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
- **Role**: Coordinates complete scraping workflow
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
     ↓                    ↓           ↓            ↓
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
│           ├── refined/      # Processed and normalized data
│           ├── reports/      # Execution logs and metadata
│           └── analysis/     # Analysis results
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

# Scrape and refine in one command (recommended)
python main.py --scrape --refine

# Scrape specific countries
python main.py --countries france germany japan

# Scrape and refine specific countries
python main.py --scrape --refine --countries france germany

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

### Refinement Pipeline Integration
The scraper now includes a sophisticated refinement pipeline that can be executed independently:

```bash
# Run complete refinement pipeline (categories + multi-value)
python main.py --refine --steps all --snapshot latest

# Run only category enrichment
python main.py --refine --steps categories --snapshot latest

# Run only multi-value splitting
python main.py --refine --steps multi-value --snapshot latest

# Run on specific snapshot
python main.py --refine --steps all --snapshot 2025-10-28
```

## Features

- **No browser automation**: Fetches JSON directly from static files
- **Automatic field discovery**: Identifies all available data fields without manual configuration
- **Historical tracking**: Stores snapshots and detects changes over time
- **Coverage analysis**: Reports which countries have which data fields
- **Multi-value handling**: Intelligently splits complex fields with multiple values
- **Category enrichment**: Maps technical database IDs to meaningful categories
- **Comprehensive logging**: Detailed execution tracking and error reporting
- **Scalable architecture**: Modular design allows for easy extension and modification

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
- `refined/`: Processed and normalized data with enriched categories and split values
- `reports/`: Execution logs, metadata, and statistics
- `analysis/`: Field catalogs and coverage analysis

## Key Insights

- **Sequential Processing**: Countries are processed one at a time to respect rate limits
- **Atomic Operations**: All file writes use temporary files and atomic renames
- **Error Recovery**: Comprehensive retry logic and error classification
- **Data Integrity**: Structure validation at every processing stage
- **Modular Design**: Each component is self-contained with clean interfaces

## Refinement Pipeline Architecture

The newly integrated refinement pipeline provides data processing capabilities:

### 1. Category Enrichment
- **Purpose**: Add rich category metadata to all country fields
- **Input**: Raw country data from `snapshot/raw/`
- **Output**: Enriched country data in `snapshot/refined/`
- **Features**: Uses 175+ category mappings from `discovery/category_mapper.py`

### 2. Multi-Value Splitting
- **Purpose**: Split fields containing `<br>` tags into structured arrays
- **Input**: Enriched country data from previous step
- **Output**: Fully refined country data in `snapshot/refined/`
- **Features**: Detects 59.2% multi-valued fields across all countries

### 3. Analysis Reporting
- **Purpose**: Generate detailed statistics on data patterns
- **Output**: Comprehensive analysis reports in `snapshot/analysis/`
- **Features**: Field-level statistics, separator pattern analysis, coverage metrics

### 4. Pipeline Orchestration
- **Sequential Processing**: Proper data flow (raw → categories → multi-value)
- **Error Handling**: Robust error recovery and progress tracking
- **Performance**: Processes 254 countries in ~1.3 seconds total
- **Flexibility**: Supports individual or combined step execution

## Pipeline Execution Results

**Latest Execution**: `python main.py --refine --steps all --snapshot latest`

**Results**:
- ✅ **254/254 countries processed** (100% success)
- ✅ **31,338 total fields** processed
- ✅ **18,557 multi-valued fields** split (59.2%)
- ✅ **12,781 single-valued fields** preserved (40.8%)
- ✅ **Processing time**: 1.3 seconds total

**Top Multi-Valued Fields Identified**:
1. Age structure: 100.0% (avg 3.0 values)
2. International environmental agreements: 100.0% (avg 2.0 values)
3. Capital: 100.0% (avg 4.5 values)
4. Nationality: 100.0% (avg 2.0 values)
5. Country name: 100.0% (avg 5.2 values)

**Analysis Reports Generated**:
- `analysis/category_report.json`: Category enrichment statistics
- `analysis/multi_value_report.json`: Multi-value pattern analysis

## Module Design Principles

1. **Single Responsibility**: Each module has one clear purpose
2. **Loose Coupling**: Modules communicate through well-defined interfaces
3. **High Cohesion**: Related functionality grouped together
4. **Easy Testing**: Individual modules can be tested in isolation
5. **Configuration Driven**: Behavior controlled through external configuration
6. **Error Resilient**: Comprehensive error handling and recovery mechanisms

## Development and Extension

The modular architecture makes it easy to extend the scraper:

1. **New Refiners**: Add modules to `refiners/` following existing patterns
2. **New Analyzers**: Add analysis modules to `analyzers/`
3. **New Data Sources**: Extend discovery modules for additional sources
4. **Custom Processing**: Modify existing modules for specific requirements
5. **Pipeline Steps**: Add new refinement steps to the orchestrator

## Error Handling

- **Network Errors**: Automatic retry with exponential backoff
- **Data Errors**: Graceful degradation with partial processing
- **File Errors**: Atomic operations with cleanup and recovery
- **Configuration Errors**: Validation with clear error messages
- **Memory Errors**: Streaming processing for large datasets

## Performance Considerations

- **Rate Limiting**: Respects server limits with configurable delays
- **Connection Pooling**: Reuses HTTP connections for efficiency
- **Memory Management**: Streaming processing for large JSON files
- **Parallel Processing**: Independent country processing for scalability
- **Caching**: Avoids redundant requests and computations
- **Atomic Operations**: Ensures data consistency during failures

# CIA Factbook Scraper Configuration

This document explains the configuration options available in `config/config.yaml` for the CIA Factbook scraper.

## Configuration Structure

The configuration is organized into several sections:

### Base URLs
- `base_url`: The base URL for the CIA Factbook website
- `sitemap_url`: URL to the sitemap XML file

### Discovery Configuration
The `discovery` section contains settings specific to the discovery process:

```yaml
discovery:
  category_mapping_urls:
    primary: "https://www.cia.gov/the-world-factbook/page-data/sq/d/2962548448.json"
    alternatives: []
  page_data_pattern: "/page-data{path}/page-data.json"
  countries_output: "data/index/countries.json"
  category_output: "data/index/category_mapping.json"
```

#### Category Mapping URLs
- `primary`: The primary URL for fetching category mapping JSON
- `alternatives`: Array of alternative URLs to try if the primary fails (useful when the hash changes)

#### URL Patterns
- `page_data_pattern`: Pattern for transforming web URLs to page-data.json URLs
  - `{path}` placeholder gets replaced with the clean path
  - Default: `/page-data{path}/page-data.json`

#### Output Paths
- `countries_output`: Where to save the countries index file
- `category_output`: Where to save the category mapping file

### Scraping Configuration
The `scraping` section contains HTTP client and request settings:

```yaml
scraping:
  retry_attempts: 3
  retry_delay: 2
  request_timeout: 30
  rate_limit_delay: 1
  user_agent: "CIA-Factbook-Scraper/1.0"
```

#### Connection Settings
- `retry_attempts`: Number of retry attempts for failed requests
- `retry_delay`: Base delay for exponential backoff (in seconds)
- `request_timeout`: Request timeout (in seconds)
- `rate_limit_delay`: Delay between requests (in seconds)
- `user_agent`: User agent string for HTTP requests

### Logging Configuration
The `logging` section controls logging behavior:

```yaml
logging:
  log_level: "DEBUG"
  log_to_file: true
  log_to_console: true
```

- `log_level`: Logging level (DEBUG, INFO, WARNING, ERROR)
- `log_to_file`: Whether to write logs to file
- `log_to_console`: Whether to output logs to console

### Snapshot Configuration
The `snapshot` section controls data archiving:

```yaml
snapshot:
  snapshot_compression: true
  archive_snapshots: false
```

- `snapshot_compression`: Whether to compress individual JSON files
- `archive_snapshots`: Whether to create tar.gz archives of old snapshots

## Managing URL Changes

### When Category Mapping Hash Changes
The CIA Factbook occasionally changes the hash in the category mapping URL. When this happens:

1. **Add the new URL as primary**:
   ```yaml
   discovery:
     category_mapping_urls:
       primary: "https://www.cia.gov/the-world-factbook/page-data/sq/d/NEW_HASH.json"
   ```

2. **Or add it to alternatives for fallback**:
   ```yaml
   discovery:
     category_mapping_urls:
       primary: "https://www.cia.gov/the-world-factbook/page-data/sq/d/OLD_HASH.json"
       alternatives:
         - "https://www.cia.gov/the-world-factbook/page-data/sq/d/NEW_HASH.json"
   ```

### When URL Patterns Change
If the CIA Factbook changes how page-data.json URLs are structured:

1. **Update the page_data_pattern**:
   ```yaml
   discovery:
     page_data_pattern: "/new-pattern{path}/data.json"
   ```

## Configuration Validation

The configuration is validated using Pydantic models. Invalid configurations will raise descriptive errors:

- Required fields must be present
- URLs must be valid strings
- Numeric values must be within reasonable ranges
- Boolean values must be true/false

## Environment-Specific Configurations

You can create different configuration files for different environments:

- `config/config.yaml` - Default configuration
- `config/development.yaml` - Development settings
- `config/production.yaml` - Production settings

Load alternative configs by setting the `CONFIG_PATH` environment variable or passing the path to the configuration loading functions.

## Migration from Hardcoded URLs

This configuration system replaces the following hardcoded values:

### Before (Hardcoded)
```python
# In discovery/category_mapper.py
DEFAULT_CATEGORY_URL = "https://www.cia.gov/the-world-factbook/page-data/sq/d/2962548448.json"

# In scrapers/fetcher.py  
USER_AGENT = "CIA-Factbook-Scraper/1.0"

# In discovery/sitemap_parser.py
output_path = "data/index/countries.json"
page_data_path = f"/page-data{clean_path}/page-data.json"
```

### After (Configured)
```python
# All values now come from config
config = load_config()
url = config.discovery.category_mapping_urls.primary
user_agent = config.scraping.user_agent
output_path = config.discovery.countries_output
page_data_path = config.discovery.page_data_pattern.format(path=clean_path)
```

## Benefits

1. **Maintainability**: Update URLs without code changes
2. **Flexibility**: Multiple fallback URLs for reliability
3. **Environment Management**: Different configs for different environments
4. **Validation**: Automatic configuration validation
5. **Documentation**: Self-documenting configuration structure

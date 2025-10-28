"""
Field Discovery & Coverage Analysis Engine for CIA Factbook Scraper.

This module analyzes all scraped country data to discover unique fields
and calculate coverage statistics for main fields only.

Key capabilities:
- Load and validate 250+ country JSON files efficiently
- Extract main field metadata with coverage tracking
- Calculate coverage percentages for main fields
- Create simplified field catalog focusing on main fields only
"""

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple, Set

from utils.logger import get_logger
from utils.config import load_config

# Module-level logger
logger = get_logger(__name__)


def discover_fields(snapshot_dir: str) -> dict:
    """
    Main entry point - discover all fields across all countries.
    
    Args:
        snapshot_dir: Path to snapshot directory
    
    Returns:
        Complete field catalog dictionary
    """
    start_time = time.time()
    logger.info(f"Starting field discovery analysis in {snapshot_dir}")
    
    # Load all country files
    raw_dir = os.path.join(snapshot_dir, 'raw')
    country_data_list = load_country_files(raw_dir)
    
    if not country_data_list:
        logger.error("No valid country data found")
        return {}
    
    total_countries = len(country_data_list)
    logger.info(f"Loaded {total_countries} countries for analysis")
    
    # Build comprehensive field registry
    field_registry = build_field_registry(country_data_list)
    
    # Calculate coverage statistics
    field_registry = calculate_coverage(field_registry, total_countries)
    
    # Generate summary statistics
    summary = generate_summary_statistics(field_registry, total_countries)
    
    # Format final catalog output
    analysis_metadata = {
        'analyzed_at': datetime.now(timezone.utc).isoformat(),
        'snapshot_date': os.path.basename(snapshot_dir),
        'total_countries_analyzed': total_countries,
        'total_unique_fields': len(field_registry),
        'analysis_duration_seconds': time.time() - start_time
    }
    
    catalog = format_catalog_output(field_registry, summary, analysis_metadata)
    
    # Save catalog to analysis directory
    output_path = os.path.join(snapshot_dir, 'analysis', 'field_catalog.json')
    save_catalog(catalog, output_path)
    
    # Print summary report
    print_summary_report(catalog, time.time() - start_time)
    
    logger.info(f"Field discovery completed in {time.time() - start_time:.2f}s")
    return catalog


def load_country_files(raw_dir: str) -> List[dict]:
    """
    Load all country JSON files from snapshot raw/ directory.
    
    Args:
        raw_dir: Path to raw/ directory containing country JSON files
    
    Returns:
        List of valid country data objects
    """
    logger.info(f"Loading country files from: {raw_dir}")
    
    if not os.path.exists(raw_dir):
        logger.error(f"Raw directory does not exist: {raw_dir}")
        return []
    
    # Get all JSON files
    json_files = [
        f for f in os.listdir(raw_dir) 
        if f.endswith('.json') and not f.startswith('.')
    ]
    
    logger.info(f"Found {len(json_files)} JSON files to process")
    
    country_data_list = []
    invalid_files = []
    
    for i, filename in enumerate(json_files, 1):
        file_path = os.path.join(raw_dir, filename)
        country_slug = filename[:-5]  # Remove .json extension
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Validate structure
            if not validate_country_data(data):
                logger.warning(f"Invalid structure in {filename}, skipping")
                invalid_files.append(filename)
                continue
            
            country_data_list.append({
                'slug': country_slug,
                'filename': filename,
                'file_path': file_path,
                'data': data
            })
            
            # Progress logging every 50 countries
            if i % 50 == 0 or i == len(json_files):
                logger.info(f"[{i}/{len(json_files)}] Processed country files...")
                
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error in {filename}: {e}")
            invalid_files.append(filename)
        except Exception as e:
            logger.error(f"Error loading {filename}: {e}")
            invalid_files.append(filename)
    
    if invalid_files:
        logger.warning(f"Skipped {len(invalid_files)} invalid files: {invalid_files[:5]}")
    
    logger.info(f"Successfully loaded {len(country_data_list)} valid country files")
    return country_data_list


def extract_field_info(field: dict, country_slug: str) -> dict:
    """
    Extract field information from a single field object.
    
    Args:
        field: Field object from country JSON
        country_slug: Country identifier for tracking
    
    Returns:
        Structured field information
    """
    return {
        'name': field.get('name'),
        'database_id': field.get('database_id'),
        'category': field.get('category'),
        'has_ranking': field.get('has_ranking', False),
        'data_length': len(field.get('data', ''))
    }


def build_field_registry(country_data_list: List[dict]) -> dict:
    """
    Build registry of all main fields across all countries.
    
    Args:
        country_data_list: List of country data objects
    
    Returns:
        Field registry keyed by field name
    """
    logger.info("Building field registry from all countries")
    start_time = time.time()
    
    field_registry = {}
    
    for country in country_data_list:
        country_slug = country['slug']
        data = country['data']
        fields = data.get('data', {}).get('fields', [])
        
        for field in fields:
            field_name = field.get('name')
            if not field_name:
                continue
            
            # Extract field information
            field_info = extract_field_info(field, country_slug)
            
            # Initialize field entry if not exists
            if field_name not in field_registry:
                field_registry[field_name] = {
                    'database_id': field_info['database_id'],
                    'category': field_info['category'],
                    'countries_with_field': [],
                    'has_ranking': field_info['has_ranking'],
                    'data_lengths': []
                }
            
            # Update field registry entry
            field_entry = field_registry[field_name]
            
            # Add country to field's country list
            field_entry['countries_with_field'].append(country_slug)
            
            # Track data characteristics
            field_entry['data_lengths'].append(field_info['data_length'])
    
    logger.info(f"Built field registry with {len(field_registry)} unique fields in {time.time() - start_time:.2f}s")
    return field_registry


def calculate_coverage(field_registry: dict, total_countries: int) -> dict:
    """
    Calculate coverage statistics for all fields.
    
    Args:
        field_registry: Field registry from build_field_registry
        total_countries: Total number of countries analyzed
    
    Returns:
        Field registry enriched with coverage statistics
    """
    logger.info("Calculating coverage statistics")
    start_time = time.time()
    
    for field_name, field_entry in field_registry.items():
        countries_with_field = field_entry['countries_with_field']
        present_count = len(countries_with_field)
        
        # Calculate coverage percentage
        coverage_percentage = (present_count / total_countries) * 100 if total_countries > 0 else 0
        
        # Identify missing countries
        missing_countries = []
        if coverage_percentage < 100:
            all_countries = set(countries_with_field)
            # This is a simplified approach - in real implementation, we'd need
            # the full list of all country slugs to compare against
            missing_countries = []  # Would be populated by comparing with full list
        
        # Update field entry with coverage info
        field_entry['coverage'] = {
            'percentage': round(coverage_percentage, 1)
        }
    
    logger.info(f"Coverage calculation completed in {time.time() - start_time:.2f}s")
    return field_registry


def generate_summary_statistics(field_registry: dict, total_countries: int) -> dict:
    """
    Generate high-level summary statistics.
    
    Args:
        field_registry: Complete field registry
        total_countries: Total countries analyzed
    
    Returns:
        Summary statistics dictionary
    """
    logger.info("Generating summary statistics")
    start_time = time.time()
    
    # Initialize summary
    summary = {
        'by_category': {},
        'coverage_distribution': {
            'universal_fields': 0,      # 100% coverage
            'common_fields': 0,          # 50-99% coverage
            'rare_fields': 0              # <50% coverage
        }
    }
    
    # Analyze by category
    category_stats = {}
    for field_name, field_entry in field_registry.items():
        category = field_entry.get('category', 'Unknown')
        if category not in category_stats:
            category_stats[category] = {
                'field_count': 0,
                'total_coverage': 0
            }
        
        category_stats[category]['field_count'] += 1
        category_stats[category]['total_coverage'] += field_entry['coverage']['percentage']
    
    # Calculate category averages
    for category, stats in category_stats.items():
        avg_coverage = stats['total_coverage'] / stats['field_count'] if stats['field_count'] > 0 else 0
        summary['by_category'][category] = {
            'field_count': stats['field_count'],
            'avg_coverage': round(avg_coverage, 1)
        }
    
    # Analyze coverage distribution
    for field_name, field_entry in field_registry.items():
        coverage = field_entry['coverage']['percentage']
        if coverage >= 100:
            summary['coverage_distribution']['universal_fields'] += 1
        elif coverage >= 50:
            summary['coverage_distribution']['common_fields'] += 1
        else:
            summary['coverage_distribution']['rare_fields'] += 1
    
    logger.info(f"Summary statistics generated in {time.time() - start_time:.2f}s")
    return summary


def format_catalog_output(field_registry: dict, summary: dict, metadata: dict) -> dict:
    """
    Format field registry into final catalog structure.
    
    Args:
        field_registry: Processed field registry
        summary: Summary statistics
        metadata: Analysis metadata
    
    Returns:
        Final catalog in output format
    """
    logger.info("Formatting catalog output")
    start_time = time.time()
    
    catalog = {
        'metadata': metadata,
        'summary': summary,
        'fields': {}
    }
    
    for field_name, field_entry in field_registry.items():
        # Calculate data characteristics
        data_lengths = field_entry['data_lengths']
        data_characteristics = {
            'min_length': min(data_lengths) if data_lengths else 0,
            'max_length': max(data_lengths) if data_lengths else 0,
            'avg_length': sum(data_lengths) / len(data_lengths) if data_lengths else 0
        }
        
        # Format field entry
        catalog['fields'][field_name] = {
            'database_id': field_entry['database_id'],
            'category': field_entry['category'],
            'coverage': field_entry['coverage']
        }
    
    logger.info(f"Catalog formatting completed in {time.time() - start_time:.2f}s")
    return catalog


def save_catalog(catalog: dict, output_path: str) -> None:
    """
    Save field catalog to JSON file.
    
    Args:
        catalog: Complete field catalog
        output_path: Path to save catalog
    """
    logger.info(f"Saving field catalog to {output_path}")
    start_time = time.time()
    
    try:
        # Ensure analysis directory exists
        analysis_dir = os.path.dirname(output_path)
        os.makedirs(analysis_dir, exist_ok=True)
        
        # Save catalog with pretty formatting
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(catalog, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Field catalog saved successfully in {time.time() - start_time:.2f}s")
        
    except Exception as e:
        logger.error(f"Failed to save field catalog: {e}")
        raise


def get_latest_snapshot() -> str:
    """
    Find the most recent snapshot directory.
    
    Returns:
        Path to latest snapshot directory
    """
    logger.info("Finding latest snapshot directory")
    
    snapshots_dir = os.path.join('data', 'snapshots')
    if not os.path.exists(snapshots_dir):
        logger.error("Snapshots directory does not exist")
        return ""
    
    # Get all snapshot directories
    snapshot_dirs = [
        d for d in os.listdir(snapshots_dir)
        if os.path.isdir(os.path.join(snapshots_dir, d)) and not d.startswith('.')
    ]
    
    if not snapshot_dirs:
        logger.error("No snapshot directories found")
        return ""
    
    # Sort by name (which should be date format) and get latest
    latest_dir = sorted(snapshot_dirs)[-1]
    latest_path = os.path.join(snapshots_dir, latest_dir)
    
    logger.info(f"Latest snapshot found: {latest_path}")
    return latest_path


def validate_country_data(data: dict) -> bool:
    """
    Validate country JSON structure.
    
    Args:
        data: Country data loaded from JSON
    
    Returns:
        True if valid, False otherwise
    """
    try:
        # Check required top-level structure
        if not isinstance(data, dict):
            return False
        
        if 'data' not in data:
            return False
        
        country_data = data['data']
        if not isinstance(country_data, dict):
            return False
        
        if 'fields' not in country_data:
            return False
        
        fields = country_data['fields']
        if not isinstance(fields, list):
            return False
        
        # Validate each field has basic structure
        for field in fields[:3]:  # Check first 3 fields for efficiency
            if not isinstance(field, dict):
                return False
            if 'name' not in field:
                return False
        
        return True
        
    except Exception as e:
        logger.debug(f"Validation error: {e}")
        return False


def print_summary_report(catalog: dict, duration: float) -> None:
    """
    Print simplified summary report to console.
    
    Args:
        catalog: Complete field catalog
        duration: Analysis duration in seconds
    """
    print(f"\n{'='*60}")
    print(f"    Field Discovery Summary")
    print(f"{'='*60}")
    print(f"")
    
    metadata = catalog['metadata']
    summary = catalog['summary']
    
    # Print metadata
    print(f"Total countries analyzed: {metadata['total_countries_analyzed']}")
    print(f"Total unique fields: {metadata['total_unique_fields']}")
    print(f"Analysis duration: {duration:.1f}s")
    print(f"")
    
    # Print coverage distribution
    coverage_dist = summary['coverage_distribution']
    print(f"Coverage Distribution:")
    print(f"  - Universal (100%): {coverage_dist['universal_fields']} fields")
    print(f"  - Common (50-99%): {coverage_dist['common_fields']} fields")
    print(f"  - Rare (<50%): {coverage_dist['rare_fields']} fields")
    print(f"")
    
    # Print category breakdown
    print(f"By Category:")
    for category, stats in sorted(summary['by_category'].items()):
        print(f"  - {category}: {stats['field_count']} fields (avg {stats['avg_coverage']}% coverage)")
    
    print(f"")
    print(f"Output saved to: {metadata['snapshot_date']}/analysis/field_catalog.json")
    print(f"{'='*60}\n")


def run(snapshot_dir: Optional[str] = None) -> dict:
    """
    Main execution - run complete field discovery.
    
    Args:
        snapshot_dir: Snapshot directory (default: latest)
    
    Returns:
        Analysis summary
    """
    start_time = time.time()
    logger.info("Starting field discovery analyzer")
    
    try:
        # Determine snapshot directory
        if not snapshot_dir:
            snapshot_dir = get_latest_snapshot()
        
        if not snapshot_dir or not os.path.exists(snapshot_dir):
            logger.error("Invalid or missing snapshot directory")
            return {}
        
        logger.info(f"Using snapshot directory: {snapshot_dir}")
        
        # Run complete field discovery
        catalog = discover_fields(snapshot_dir)
        
        logger.info(f"Field discovery completed in {time.time() - start_time:.2f}s")
        return catalog
        
    except Exception as e:
        logger.error(f"Field discovery failed: {e}")
        raise


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Field Discovery & Coverage Analysis for CIA Factbook',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m analyzers.field_discovery                    # Analyze latest snapshot
  python -m analyzers.field_discovery --snapshot 2025-10-28  # Analyze specific snapshot
        """
    )
    
    parser.add_argument(
        '--snapshot',
        type=str,
        help='Snapshot directory to analyze (default: latest)'
    )
    
    args = parser.parse_args()
    
    try:
        result = run(snapshot_dir=args.snapshot)
        print(f"\nField discovery completed successfully!")
        
    except KeyboardInterrupt:
        print("\nField discovery interrupted by user.")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        print(f"Field discovery failed: {e}")

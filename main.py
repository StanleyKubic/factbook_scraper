#!/usr/bin/env python3
"""
CIA Factbook Scraper Orchestrator

Coordinates complete scraping workflow: fetch, parse, merge, and store data for all countries.
This is the main entry point for the scraping system.
"""

import argparse
import json
import os
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scrapers.fetcher import fetch_page_data
from scrapers.parser import parse_country_data
from utils.config import load_config
from utils.logger import get_logger
from discovery.category_mapper import fetch_category_mapping, save_category_mapping
from refiners.category_enricher import run as run_category_enrichment
from refiners.multi_value_splitter import run as run_multi_value_splitter

# Module-level logger
logger = get_logger(__name__)


def run_scraper(
    snapshot_date: Optional[str] = None,
    country_filter: Optional[List[str]] = None,
    dry_run: bool = False
) -> Dict[str, Any]:
    """
    Main entry point - orchestrate complete scraping workflow.
    
    Args:
        snapshot_date: Override snapshot date (default: today YYYY-MM-DD)
        country_filter: Scrape only specific countries (for testing)
        dry_run: Test without saving files
    
    Returns:
        Summary statistics dictionary
    """
    start_time = datetime.now(timezone.utc)
    logger.info(f"Starting CIA Factbook scraper orchestrator at {start_time.isoformat()}")
    
    try:
        # Load configuration and countries
        config = load_config()
        countries = load_countries()
        
        # Apply country filter if provided
        if country_filter:
            filtered_countries = []
            for country in countries:
                if country['slug'] in country_filter:
                    filtered_countries.append(country)
            countries = filtered_countries
            logger.info(f"Filtered to {len(countries)} countries: {country_filter}")
        
        # Create snapshot directory structure
        snapshot_dir = create_snapshot_directory(snapshot_date)
        logger.info(f"Created snapshot directory: {snapshot_dir}")
        
        # Initialize tracking
        scrape_results = []
        successful_scrapes = 0
        failed_scrapes = 0
        
        logger.info(f"Starting sequential scrape of {len(countries)} countries")
        print(f"\n{'='*60}")
        print(f"    CIA Factbook Scraping Started")
        print(f"{'='*60}")
        print(f"Total Countries: {len(countries)}")
        print(f"Output Directory: {snapshot_dir}")
        print(f"{'='*60}\n")
        
        # Sequential processing
        for i, country in enumerate(countries, 1):
            try:
                # Scrape single country
                result = scrape_country(country)
                scrape_results.append(result)
                
                # Save country data (unless dry run)
                if not dry_run and result['success']:
                    save_country_data(result, snapshot_dir)
                
                # Update progress display
                if result['success']:
                    successful_scrapes += 1
                else:
                    failed_scrapes += 1
                
                print_progress(
                    current=i,
                    total=len(countries),
                    country_slug=result['slug'],
                    success=result['success'],
                    start_time=start_time
                )
                
            except Exception as e:
                logger.error(f"Unexpected error processing {country['slug']}: {e}")
                failed_scrapes += 1
                continue
        
        # Generate metadata and logs (unless dry run)
        if not dry_run:
            end_time = datetime.now(timezone.utc)
            generate_metadata(scrape_results, snapshot_dir, start_time, end_time)
            generate_scrape_log(scrape_results, snapshot_dir)
        
        # Print final summary
        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        print_summary(scrape_results, duration)
        
        # Return summary statistics
        summary = {
            'snapshot_date': snapshot_dir.split('/')[-2] if snapshot_dir else snapshot_date,
            'total_countries': len(countries),
            'successful_scrapes': successful_scrapes,
            'failed_scrapes': failed_scrapes,
            'success_rate': successful_scrapes / len(countries) if countries else 0,
            'duration_seconds': duration,
            'snapshot_directory': snapshot_dir if not dry_run else None
        }
        
        logger.info(f"Scraping completed: {summary}")
        return summary
        
    except Exception as e:
        logger.error(f"Critical error in orchestrator: {e}")
        raise


def scrape_country(country: Dict[str, Any]) -> Dict[str, Any]:
    """
    Scrape single country (fetch + parse only).
    
    Args:
        country: Country object from countries.json
    
    Returns:
        Scrape result with data or error
    """
    slug = country['slug']
    source_url = country['urls']['main']
    
    if not source_url:
        return {
            'slug': slug,
            'success': False,
            'scraped_at': datetime.now(timezone.utc).isoformat(),
            'source_url': None,
            'duration_seconds': 0,
            'error': 'no_main_url',
            'error_details': 'No main URL found in countries.json'
        }
    
    start_time = time.time()
    scraped_at = datetime.now(timezone.utc).isoformat()
    
    logger.debug(f"Starting scrape for {slug} from {source_url}")
    
    try:
        # Fetch page data
        page_data = fetch_page_data(source_url)
        
        if page_data is None:
            return {
                'slug': slug,
                'success': False,
                'scraped_at': scraped_at,
                'source_url': source_url,
                'duration_seconds': time.time() - start_time,
                'error': 'fetch_failed',
                'error_details': 'Failed to fetch page data'
            }
        
        # Parse country data
        parsed_data = parse_country_data(page_data, source_url)
        
        # Check if parsing failed
        if 'error' in parsed_data.get('metadata', {}):
            return {
                'slug': slug,
                'success': False,
                'scraped_at': scraped_at,
                'source_url': source_url,
                'duration_seconds': time.time() - start_time,
                'error': 'parse_failed',
                'error_details': parsed_data['metadata']['error']
            }
        
        duration = time.time() - start_time
        logger.debug(f"Successfully scraped {slug} in {duration:.2f}s")
        
        return {
            'slug': slug,
            'success': True,
            'scraped_at': scraped_at,
            'source_url': source_url,
            'duration_seconds': duration,
            'data': parsed_data
        }
        
    except Exception as e:
        logger.error(f"Error scraping {slug}: {e}")
        return {
            'slug': slug,
            'success': False,
            'scraped_at': scraped_at,
            'source_url': source_url,
            'duration_seconds': time.time() - start_time,
            'error': 'unexpected_error',
            'error_details': str(e)
        }


def save_country_data(scrape_result: Dict[str, Any], snapshot_dir: str) -> None:
    """
    Save country data to file with atomic write.
    
    Args:
        scrape_result: Result from scrape_country()
        snapshot_dir: Snapshot directory path
    """
    if not scrape_result['success']:
        logger.debug(f"Skipping save for failed country: {scrape_result['slug']}")
        return
    
    try:
        # Construct output path
        raw_dir = os.path.join(snapshot_dir, 'raw')
        output_path = os.path.join(raw_dir, f"{scrape_result['slug']}.json")
        
        # Prepare wrapper data
        wrapper_data = {
            'country_slug': scrape_result['slug'],
            'scraped_at': scrape_result['scraped_at'],
            'source_url': scrape_result['source_url'],
            'scrape_success': True,
            'data': scrape_result['data']
        }
        
        # Atomic write: write to temp file first, then rename
        with tempfile.NamedTemporaryFile(
            mode='w',
            suffix='.tmp',
            dir=raw_dir,
            delete=False,
            encoding='utf-8'
        ) as temp_file:
            json.dump(wrapper_data, temp_file, indent=2, ensure_ascii=False)
            temp_path = temp_file.name
        
        # Atomic rename
        os.rename(temp_path, output_path)
        
        logger.debug(f"Saved {scrape_result['slug']} data to {output_path}")
        
    except Exception as e:
        logger.error(f"Failed to save {scrape_result['slug']}: {e}")
        raise


def create_snapshot_directory(date_str: Optional[str] = None) -> str:
    """
    Create dated snapshot directory structure.
    
    Args:
        date_str: Date string (default: today YYYY-MM-DD)
    
    Returns:
        Path to created snapshot directory
    """
    if date_str is None:
        date_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    
    # Create directory structure
    base_dir = os.path.join('data', 'snapshots', date_str)
    raw_dir = os.path.join(base_dir, 'raw')
    reports_dir = os.path.join(base_dir, 'reports')
    
    # Create directories if they don't exist
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(reports_dir, exist_ok=True)
    
    return base_dir


def generate_metadata(
    scrape_results: List[Dict[str, Any]],
    snapshot_dir: str,
    start_time: datetime,
    end_time: datetime
) -> None:
    """
    Generate metadata.json with run statistics.
    
    Args:
        scrape_results: All country scrape results
        snapshot_dir: Snapshot directory path
        start_time: Scrape start timestamp
        end_time: Scrape end timestamp
    """
    try:
        # Calculate statistics
        total_countries = len(scrape_results)
        successful_scrapes = sum(1 for r in scrape_results if r['success'])
        failed_scrapes = total_countries - successful_scrapes
        
        # Collect failed countries
        failed_countries = [
            r['slug'] for r in scrape_results 
            if not r['success']
        ]
        
        # Calculate field statistics
        total_fields = 0
        total_assets = 0
        for result in scrape_results:
            if result['success'] and 'data' in result:
                data = result['data']
                total_fields += len(data.get('fields', []))
                
                # Count assets
                assets = data.get('assets', {})
                total_assets += sum(1 for key, value in assets.items() if value)
        
        # Load config for reference
        config = load_config()
        
        # Create metadata
        metadata = {
            'snapshot_date': snapshot_dir.split('/')[-2],
            'scrape_started_at': start_time.isoformat(),
            'scrape_completed_at': end_time.isoformat(),
            'duration_seconds': (end_time - start_time).total_seconds(),
            'total_countries': total_countries,
            'successful_scrapes': successful_scrapes,
            'failed_scrapes': failed_scrapes,
            'success_rate': successful_scrapes / total_countries if total_countries > 0 else 0,
            'failed_countries': failed_countries,
            'config_used': {
                'retry_attempts': config.scraping.retry_attempts,
                'rate_limit_delay': config.scraping.rate_limit_delay,
                'request_timeout': config.scraping.request_timeout
            },
            'statistics': {
                'total_fields_scraped': total_fields,
                'avg_fields_per_country': total_fields / successful_scrapes if successful_scrapes > 0 else 0,
                'total_assets_found': total_assets
            }
        }
        
        # Write metadata file
        metadata_path = os.path.join(snapshot_dir, 'metadata.json')
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Generated metadata.json with {successful_scrapes}/{total_countries} successful scrapes")
        
    except Exception as e:
        logger.error(f"Failed to generate metadata: {e}")
        raise


def generate_scrape_log(scrape_results: List[Dict[str, Any]], snapshot_dir: str) -> None:
    """
    Generate detailed scrape log.
    
    Args:
        scrape_results: All country scrape results
        snapshot_dir: Snapshot directory path
    """
    try:
        # Transform results into log entries
        log_entries = []
        for result in scrape_results:
            entry = {
                'country_slug': result['slug'],
                'timestamp': result['scraped_at'],
                'status': 'success' if result['success'] else 'failed',
                'duration_seconds': result['duration_seconds'],
                'source_url': result['source_url']
            }
            
            # Add error details if failed
            if not result['success']:
                entry['error'] = result.get('error', 'unknown')
                entry['error_details'] = result.get('error_details', '')
            else:
                # Add field count for successful scrapes
                if 'data' in result:
                    entry['fields_count'] = len(result['data'].get('fields', []))
            
            log_entries.append(entry)
        
        # Create log structure
        scrape_log = {
            'log_entries': log_entries
        }
        
        # Write log file
        log_path = os.path.join(snapshot_dir, 'reports', 'scrape_log.json')
        with open(log_path, 'w', encoding='utf-8') as f:
            json.dump(scrape_log, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Generated scrape log with {len(log_entries)} entries")
        
    except Exception as e:
        logger.error(f"Failed to generate scrape log: {e}")
        raise


def print_progress(
    current: int,
    total: int,
    country_slug: str,
    success: bool,
    start_time: datetime
) -> None:
    """
    Display progress during scraping.
    
    Args:
        current: Current country number
        total: Total countries
        country_slug: Current country being scraped
        success: Whether scrape succeeded
        start_time: Scrape start time
    """
    # Calculate progress
    percentage = (current / total) * 100
    elapsed = time.time() - start_time.timestamp()
    
    # Calculate ETA
    if current > 0:
        avg_time_per_country = elapsed / current
        remaining_countries = total - current
        eta_seconds = remaining_countries * avg_time_per_country
        eta_minutes = int(eta_seconds // 60)
        eta_seconds_remaining = int(eta_seconds % 60)
        eta_str = f"{eta_minutes}m {eta_seconds_remaining}s"
    else:
        eta_str = "N/A"
    
    # Format elapsed time
    elapsed_minutes = int(elapsed // 60)
    elapsed_seconds = int(elapsed % 60)
    elapsed_str = f"{elapsed_minutes}m {elapsed_seconds}s"
    
    # Status indicator
    status = "✓" if success else "✗"
    error_info = ""
    
    if not success:
        # Find the error details from recent results (simplified)
        error_info = " (failed)"
    
    # Print progress line
    progress_line = f"[{current}/{total}] ({percentage:.1f}%) {country_slug} {status}{error_info} | Elapsed: {elapsed_str} | ETA: {eta_str}"
    
    # Use carriage return to overwrite same line (except for last item)
    if current < total:
        print(f"\r{progress_line}", end='', flush=True)
    else:
        print(f"\r{progress_line}")


def print_summary(scrape_results: List[Dict[str, Any]], duration: float) -> None:
    """
    Print summary report at end of scraping.
    
    Args:
        scrape_results: All country scrape results
        duration: Total duration in seconds
    """
    total_countries = len(scrape_results)
    successful_scrapes = sum(1 for r in scrape_results if r['success'])
    failed_scrapes = total_countries - successful_scrapes
    success_rate = (successful_scrapes / total_countries * 100) if total_countries > 0 else 0
    
    # Format duration
    minutes = int(duration // 60)
    seconds = int(duration % 60)
    duration_str = f"{minutes}m {seconds}s"
    
    # Collect failed countries with errors
    failed_countries = []
    for result in scrape_results:
        if not result['success']:
            error = result.get('error', 'unknown')
            failed_countries.append(f"  - {result['slug']}: {error}")
    
    # Calculate statistics
    total_fields = 0
    for result in scrape_results:
        if result['success'] and 'data' in result:
            total_fields += len(result['data'].get('fields', []))
    
    avg_fields = total_fields // successful_scrapes if successful_scrapes > 0 else 0
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"    CIA Factbook Scrape Complete")
    print(f"{'='*60}")
    print(f"")
    print(f"Duration: {duration_str}")
    print(f"Total Countries: {total_countries}")
    print(f"")
    print(f"Successful: {successful_scrapes} ({success_rate:.1f}%)")
    print(f"Failed: {failed_scrapes} ({100 - success_rate:.1f}%)")
    
    if failed_countries:
        print(f"\nFailed Countries:")
        for country in failed_countries[:10]:  # Show first 10 failed countries
            print(country)
        if len(failed_countries) > 10:
            print(f"  ... and {len(failed_countries) - 10} more")
    
    print(f"\nStatistics:")
    print(f"  Total Fields Scraped: {total_fields:,}")
    print(f"  Avg Fields/Country: {avg_fields}")
    
    # Find snapshot directory
    snapshot_dirs = [d for d in os.listdir('data/snapshots') if d != '.gitkeep']
    if snapshot_dirs:
        latest_snapshot = sorted(snapshot_dirs)[-1]
        print(f"\nOutput: data/snapshots/{latest_snapshot}/")
    
    print(f"{'='*60}\n")


def load_category_mapping() -> Dict[str, str]:
    """
    Load category mapping from category_mapping.json file.
    
    Returns:
        Dictionary mapping database_id to category
    """
    try:
        categories_path = os.path.join('data', 'index', 'category_mapping.json')
        with open(categories_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        mapping = data.get('mapping', {})
        logger.info(f"Loaded {len(mapping)} category mappings from {categories_path}")
        return mapping
        
    except FileNotFoundError:
        logger.warning("Category mapping file not found. Run category discovery first.")
        return {}
    except Exception as e:
        logger.error(f"Failed to load category mapping: {e}")
        return {}


def enrich_with_categories(country_data: Dict[str, Any], category_mapping: Dict[str, str]) -> Dict[str, Any]:
    """
    Enrich country data with category information based on database_id.
    
    Args:
        country_data: Parsed country data from parser
        category_mapping: Database ID to category mapping
    
    Returns:
        Enriched country data with category field added to each field
    """
    try:
        # Get fields from country data
        fields = country_data.get('fields', [])
        enriched_fields = []
        
        for field in fields:
            enriched_field = field.copy()
            database_id = field.get('database_id')
            
            # Add category if mapping exists
            if database_id and database_id in category_mapping:
                enriched_field['category'] = category_mapping[database_id]
            else:
                enriched_field['category'] = None
            
            enriched_fields.append(enriched_field)
        
        # Update country data with enriched fields
        enriched_data = country_data.copy()
        enriched_data['fields'] = enriched_fields
        
        # Count fields with categories
        fields_with_categories = sum(1 for f in enriched_fields if f.get('category'))
        logger.debug(f"Enriched {fields_with_categories}/{len(enriched_fields)} fields with categories")
        
        return enriched_data
        
    except Exception as e:
        logger.error(f"Failed to enrich with categories: {e}")
        return country_data


def load_countries() -> List[Dict[str, Any]]:
    """
    Load countries list from countries.json.
    
    Returns:
        List of country objects
    """
    try:
        countries_path = os.path.join('data', 'index', 'countries.json')
        with open(countries_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        countries = data.get('countries', [])
        logger.info(f"Loaded {len(countries)} countries from {countries_path}")
        return countries
        
    except Exception as e:
        logger.error(f"Failed to load countries: {e}")
        raise


def run_refinement_pipeline(
    snapshot_date: Optional[str] = None,
    steps: Optional[List[str]] = None,
    country_filter: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Run refinement pipeline on existing snapshot.
    
    Args:
        snapshot_date: Snapshot date (default: latest)
        steps: Refinement steps to run (categories, multi-value, all)
        country_filter: Process only specific countries
    
    Returns:
        Summary statistics dictionary
    """
    start_time = time.time()
    
    try:
        # Determine snapshot directory
        if snapshot_date:
            if snapshot_date == 'latest':
                snapshot_dir = get_latest_snapshot()
            else:
                snapshot_dir = os.path.join('data', 'snapshots', snapshot_date)
        else:
            snapshot_dir = get_latest_snapshot()
        
        if not os.path.exists(snapshot_dir):
            raise FileNotFoundError(f"Snapshot directory {snapshot_dir} not found")
        
        # Default to all steps if none specified
        if steps is None:
            steps = ['all']
        
        # Normalize steps
        if 'all' in steps:
            steps_to_run = ['categories', 'multi-value']
        else:
            steps_to_run = steps
        
        logger.info(f"Starting refinement pipeline for snapshot: {snapshot_dir}")
        logger.info(f"Steps to run: {steps_to_run}")
        
        print(f"\n{'='*60}")
        print(f"    Refinement Pipeline Started")
        print(f"{'='*60}")
        print(f"Snapshot: {snapshot_dir}")
        print(f"Steps: {', '.join(steps_to_run)}")
        print(f"{'='*60}\n")
        
        # Define directories
        raw_dir = os.path.join(snapshot_dir, 'raw')
        refined_dir = os.path.join(snapshot_dir, 'refined')
        
        # Check if raw directory exists
        if not os.path.exists(raw_dir):
            raise FileNotFoundError(f"Raw directory {raw_dir} not found")
        
        # Initialize results tracking
        pipeline_results = {
            'snapshot_directory': snapshot_dir,
            'steps_executed': [],
            'step_results': {},
            'total_duration_seconds': 0
        }
        
        # Step 1: Category Enrichment
        if 'categories' in steps_to_run:
            logger.info("Starting category enrichment step...")
            
            category_result = run_category_enrichment(raw_dir, refined_dir)
            pipeline_results['steps_executed'].append('categories')
            pipeline_results['step_results']['categories'] = category_result
            
            print(f"\n✓ Category enrichment completed in {category_result['duration_seconds']:.1f}s")
        
        # Step 2: Multi-Value Splitting
        if 'multi-value' in steps_to_run:
            logger.info("Starting multi-value splitting step...")
            
            # Determine input directory based on whether categories were processed
            if 'categories' in steps_to_run:
                mv_input_dir = refined_dir  # Read from enriched data
            else:
                mv_input_dir = raw_dir     # Read from raw data
            
            multi_value_result = run_multi_value_splitter(mv_input_dir, refined_dir)
            pipeline_results['steps_executed'].append('multi-value')
            pipeline_results['step_results']['multi-value'] = multi_value_result
            
            print(f"\n✓ Multi-value splitting completed in {multi_value_result['duration_seconds']:.1f}s")
        
        # Calculate total duration
        pipeline_results['total_duration_seconds'] = time.time() - start_time
        
        # Print final summary
        print(f"\n{'='*60}")
        print(f"    Refinement Pipeline Complete")
        print(f"{'='*60}")
        print(f"Total duration: {pipeline_results['total_duration_seconds']:.1f} seconds")
        print(f"Steps executed: {', '.join(pipeline_results['steps_executed'])}")
        print(f"Output directory: {refined_dir}/")
        
        if country_filter:
            print(f"Countries filtered: {len(country_filter)}")
        
        print(f"{'='*60}\n")
        
        logger.info(f"Refinement pipeline completed successfully in {pipeline_results['total_duration_seconds']:.1f}s")
        return pipeline_results
        
    except Exception as e:
        logger.error(f"Refinement pipeline failed: {e}")
        raise


def get_latest_snapshot() -> str:
    """
    Find most recent snapshot directory.
    
    Returns:
        Path to latest snapshot directory
    """
    snapshots_dir = "data/snapshots"
    
    try:
        if not os.path.exists(snapshots_dir):
            raise FileNotFoundError(f"Snapshots directory {snapshots_dir} not found")
        
        # Get all subdirectories (ignore .gitkeep)
        snapshot_dirs = [
            d for d in os.listdir(snapshots_dir) 
            if os.path.isdir(os.path.join(snapshots_dir, d)) and d != '.gitkeep'
        ]
        
        if not snapshot_dirs:
            raise ValueError("No snapshot directories found")
        
        # Sort by date (YYYY-MM-DD format should sort correctly)
        latest_snapshot = sorted(snapshot_dirs)[-1]
        latest_path = os.path.join(snapshots_dir, latest_snapshot)
        
        logger.info(f"Found latest snapshot: {latest_path}")
        return latest_path
        
    except Exception as e:
        logger.error(f"Failed to find latest snapshot: {e}")
        raise


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='CIA Factbook Scraper Orchestrator',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Scraping workflows
  python main.py --scrape                                          # Scrape all countries
  python main.py --scrape --countries france germany               # Scrape specific countries
  python main.py --scrape --date 2025-10-26                     # Override snapshot date
  python main.py --scrape --dry-run                              # Test without saving
  
  # Refinement workflows
  python main.py --refine                                          # Refine latest snapshot (all steps)
  python main.py --refine --steps categories --snapshot 2025-10-28  # Specific step and snapshot
  python main.py --refine --steps multi-value                       # Only multi-value splitting
  python main.py --refine --steps all --snapshot latest              # All steps on latest
  
  # Complete pipeline
  python main.py --scrape --refine                                 # Scrape then refine
        """
    )
    
    # Mode selection (can be used together or separately)
    parser.add_argument(
        '--scrape',
        action='store_true',
        help='Run scraping workflow'
    )
    parser.add_argument(
        '--refine',
        action='store_true',
        help='Run refinement workflow'
    )
    
    # Scraping arguments
    parser.add_argument(
        '--countries',
        nargs='+',
        help='Scrape only specific countries (slugs)'
    )
    
    parser.add_argument(
        '--date',
        type=str,
        help='Override snapshot date (YYYY-MM-DD format)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Test run without saving files (scraping only)'
    )
    
    # Refinement arguments
    parser.add_argument(
        '--steps',
        nargs='+',
        choices=['categories', 'multi-value', 'all'],
        help='Refinement steps to run: categories, multi-value, all (default: all)'
    )
    
    parser.add_argument(
        '--snapshot',
        type=str,
        help='Snapshot date to refine (YYYY-MM-DD format or "latest")'
    )
    
    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_arguments()
    
    try:
        # Validate that at least one workflow flag is provided
        if not args.scrape and not args.refine:
            print("Error: At least one of --scrape or --refine must be specified.")
            print("Use --help for usage information.")
            sys.exit(1)
        
        # Validate date format if provided
        if args.date:
            try:
                datetime.strptime(args.date, '%Y-%m-%d')
            except ValueError:
                print(f"Error: Invalid date format '{args.date}'. Use YYYY-MM-DD format.")
                sys.exit(1)
        
        # Validate snapshot format if provided for refinement
        if args.refine and args.snapshot and args.snapshot != 'latest':
            try:
                datetime.strptime(args.snapshot, '%Y-%m-%d')
            except ValueError:
                print(f"Error: Invalid snapshot format '{args.snapshot}'. Use YYYY-MM-DD format or 'latest'.")
                sys.exit(1)
        
        # Run appropriate workflow
        if args.scrape:
            # Run scraping workflow
            scrape_summary = run_scraper(
                snapshot_date=args.date,
                country_filter=args.countries,
                dry_run=args.dry_run
            )
            
            # If refinement is also requested, run it after scraping
            if args.refine:
                print(f"\n{'='*60}")
                print(f"    Starting Refinement After Scraping")
                print(f"{'='*60}")
                
                # Use the snapshot that was just created
                if scrape_summary.get('snapshot_directory'):
                    snapshot_date = os.path.basename(scrape_summary['snapshot_directory'])
                    refine_summary = run_refinement_pipeline(
                        snapshot_date=snapshot_date,
                        steps=args.steps,
                        country_filter=args.countries
                    )
                    
                    # Exit based on both workflows
                    total_failed = scrape_summary.get('failed_scrapes', 0) + \
                                 len(refine_summary.get('step_results', {}).get('multi-value', {}).get('processing_stats', {}).get('failed_countries', []))
                    sys.exit(0 if total_failed == 0 else 1)
                else:
                    print("Warning: No snapshot directory created, skipping refinement.")
                    sys.exit(0 if scrape_summary.get('failed_scrapes', 0) == 0 else 1)
            else:
                # Exit based on scraping only
                sys.exit(0 if scrape_summary.get('failed_scrapes', 0) == 0 else 1)
        
        elif args.refine:
            # Run refinement workflow only
            refine_summary = run_refinement_pipeline(
                snapshot_date=args.snapshot,
                steps=args.steps,
                country_filter=args.countries
            )
            
            # Exit based on refinement results
            total_failed = len(refine_summary.get('step_results', {}).get('multi-value', {}).get('processing_stats', {}).get('failed_countries', []))
            sys.exit(0 if total_failed == 0 else 1)
        
    except KeyboardInterrupt:
        print("\nOperation interrupted by user.")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        print(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()

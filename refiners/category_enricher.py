"""
Category Enricher for CIA Factbook Data

This module enriches raw country data by adding category information
to each field based on database_id mapping from category_mapping.json.

Author: CIA Factbook Scraper
"""

import json
import os
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any

from utils.logger import get_logger

# Module-level logger
logger = get_logger(__name__)


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
        country_data: Raw country data from raw/ directory
        category_mapping: Database ID to category mapping
    
    Returns:
        Enriched country data with category field added to each field
    """
    try:
        # Extract the actual data from wrapper structure
        raw_data = country_data.get('data', {})
        fields = raw_data.get('fields', [])
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
        
        # Update raw data with enriched fields
        enriched_data = raw_data.copy()
        enriched_data['fields'] = enriched_fields
        
        # Count fields with categories
        fields_with_categories = sum(1 for f in enriched_fields if f.get('category'))
        total_fields = len(enriched_fields)
        
        logger.debug(f"Enriched {fields_with_categories}/{total_fields} fields with categories")
        
        # Return wrapper structure with enriched data
        result = country_data.copy()
        result['data'] = enriched_data
        result['enriched_at'] = datetime.now(timezone.utc).isoformat()
        result['enrichment_stats'] = {
            'total_fields': total_fields,
            'fields_with_categories': fields_with_categories,
            'category_coverage_percentage': round((fields_with_categories / total_fields * 100) if total_fields > 0 else 0, 1)
        }
        
        return result
        
    except Exception as e:
        logger.error(f"Failed to enrich with categories: {e}")
        return country_data


def refine_country(country_data: Dict[str, Any], category_mapping: Dict[str, str]) -> Dict[str, Any]:
    """
    Transform single country from raw to enriched format.
    
    Args:
        country_data: Country data from raw/ directory
        category_mapping: Database ID to category mapping
    
    Returns:
        Enriched country structure
    """
    start_time = time.time()
    
    # Extract country metadata
    country_slug = country_data.get('country_slug', '')
    
    # Apply category enrichment
    enriched_data = enrich_with_categories(country_data, category_mapping)
    
    # Add processing metadata
    enriched_data['processing_duration_seconds'] = time.time() - start_time
    
    logger.debug(f"Enriched {country_slug} with categories in {enriched_data['processing_duration_seconds']:.2f}s")
    
    return enriched_data


def save_enriched_country(enriched_data: Dict[str, Any], output_path: str) -> None:
    """
    Save enriched country data to JSON file with atomic write.
    
    Args:
        enriched_data: Enriched country data
        output_path: Path to save file
    """
    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Atomic write: write to temp file first, then rename
        with tempfile.NamedTemporaryFile(
            mode='w',
            suffix='.tmp',
            dir=os.path.dirname(output_path),
            delete=False,
            encoding='utf-8'
        ) as temp_file:
            json.dump(enriched_data, temp_file, indent=2, ensure_ascii=False)
            temp_path = temp_file.name
        
        # Atomic rename
        os.rename(temp_path, output_path)
        
        logger.debug(f"Saved enriched data to {output_path}")
        
    except Exception as e:
        logger.error(f"Failed to save enriched data to {output_path}: {e}")
        raise


def load_country_files(directory: str) -> List[Dict[str, Any]]:
    """
    Load all country JSON files from directory.
    
    Args:
        directory: Directory path containing country files
    
    Returns:
        List of country data objects
    """
    country_files = []
    
    try:
        if not os.path.exists(directory):
            raise FileNotFoundError(f"Directory {directory} not found")
        
        for filename in os.listdir(directory):
            if filename.endswith('.json'):
                file_path = os.path.join(directory, filename)
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        country_data = json.load(f)
                        country_files.append(country_data)
                except Exception as e:
                    logger.error(f"Failed to load {file_path}: {e}")
                    continue
        
        logger.info(f"Loaded {len(country_files)} country files from {directory}")
        return country_files
        
    except Exception as e:
        logger.error(f"Failed to scan directory {directory}: {e}")
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


def process_all_countries(input_dir: str, output_dir: str) -> Dict[str, Any]:
    """
    Process all countries in input directory with category enrichment.
    
    Args:
        input_dir: Path to input directory (raw/ or refined/)
        output_dir: Path to output directory (refined/)
    
    Returns:
        Processing summary with statistics
    """
    start_time = time.time()
    
    # Load category mapping
    category_mapping = load_category_mapping()
    if not category_mapping:
        raise ValueError("No category mapping available. Run category discovery first.")
    
    # Create output directory if not exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Load all country files
    logger.info(f"Loading countries from: {input_dir}")
    country_files = load_country_files(input_dir)
    
    if not country_files:
        raise ValueError(f"No country files found in {input_dir}")
    
    # Initialize statistics tracking
    processing_stats = {
        "total_countries": len(country_files),
        "successful_countries": 0,
        "failed_countries": [],
        "total_fields_processed": 0,
        "total_fields_with_categories": 0,
        "processing_start": datetime.now(timezone.utc).isoformat(),
        "processing_duration_seconds": 0
    }
    
    logger.info(f"Starting category enrichment for {len(country_files)} countries...")
    print(f"\nProcessing countries for category enrichment...")
    
    # Process each country
    for i, country_data in enumerate(country_files, 1):
        try:
            country_slug = country_data.get('country_slug', 'unknown')
            
            # Enrich country data
            enriched_data = refine_country(country_data, category_mapping)
            
            # Save enriched data
            output_path = os.path.join(output_dir, f"{country_slug}.json")
            save_enriched_country(enriched_data, output_path)
            
            # Update statistics
            processing_stats["successful_countries"] += 1
            processing_stats["total_fields_processed"] += enriched_data.get('enrichment_stats', {}).get('total_fields', 0)
            processing_stats["total_fields_with_categories"] += enriched_data.get('enrichment_stats', {}).get('fields_with_categories', 0)
            
            # Progress logging
            if i % 50 == 0 or i == len(country_files):
                print(f"[{i}/{len(country_files)}] Processed {i} countries...")
                
        except Exception as e:
            country_slug = country_data.get('country_slug', 'unknown')
            logger.error(f"Failed to enrich {country_slug}: {e}")
            processing_stats["failed_countries"].append(country_slug)
            continue
    
    # Calculate final statistics
    processing_stats["processing_duration_seconds"] = time.time() - start_time
    processing_stats["processing_end"] = datetime.now(timezone.utc).isoformat()
    
    # Log summary
    success_rate = processing_stats["successful_countries"] / processing_stats["total_countries"] * 100
    category_coverage_rate = (processing_stats["total_fields_with_categories"] / processing_stats["total_fields_processed"] * 100) if processing_stats["total_fields_processed"] > 0 else 0
    
    logger.info(f"Category enrichment complete: {processing_stats['successful_countries']}/{processing_stats['total_countries']} countries ({success_rate:.1f}% success)")
    logger.info(f"Fields processed: {processing_stats['total_fields_processed']}, With categories: {processing_stats['total_fields_with_categories']} ({category_coverage_rate:.1f}% coverage)")
    
    return processing_stats


def run(input_dir: Optional[str] = None, output_dir: Optional[str] = None) -> Dict[str, Any]:
    """
    Main execution - process all countries with category enrichment.
    
    Args:
        input_dir: Input directory (default: latest snapshot/raw/)
        output_dir: Output directory (default: latest snapshot/refined/)
    
    Returns:
        Processing summary dictionary
    """
    start_time = time.time()
    
    try:
        # Determine directories
        if input_dir is None:
            snapshot_dir = get_latest_snapshot()
            input_dir = os.path.join(snapshot_dir, 'raw')
        
        if output_dir is None:
            if 'snapshot_dir' not in locals():
                snapshot_dir = get_latest_snapshot()
            output_dir = os.path.join(snapshot_dir, 'refined')
        
        logger.info(f"Starting category enrichment")
        print(f"\n{'='*60}")
        print(f"    Category Enrichment Started")
        print(f"{'='*60}")
        print(f"Input: {input_dir}")
        print(f"Output: {output_dir}")
        print(f"{'='*60}\n")
        
        # Process all countries
        processing_stats = process_all_countries(input_dir, output_dir)
        
        # Print summary statistics
        duration = time.time() - start_time
        success_rate = processing_stats["successful_countries"] / processing_stats["total_countries"] * 100
        category_coverage_rate = (processing_stats["total_fields_with_categories"] / processing_stats["total_fields_processed"] * 100) if processing_stats["total_fields_processed"] > 0 else 0
        
        print(f"\n{'='*60}")
        print(f"    Category Enrichment Complete")
        print(f"{'='*60}")
        print(f"Duration: {duration:.1f} seconds")
        print(f"Countries processed: {processing_stats['successful_countries']}/{processing_stats['total_countries']} ({success_rate:.1f}% success)")
        print(f"Total fields: {processing_stats['total_fields_processed']:,}")
        print(f"Fields with categories: {processing_stats['total_fields_with_categories']:,} ({category_coverage_rate:.1f}% coverage)")
        
        if processing_stats['failed_countries']:
            print(f"\nFailed countries: {len(processing_stats['failed_countries'])}")
            for country in processing_stats['failed_countries'][:5]:
                print(f"  - {country}")
            if len(processing_stats['failed_countries']) > 5:
                print(f"  ... and {len(processing_stats['failed_countries']) - 5} more")
        
        print(f"\nOutput:")
        print(f"  Enriched files: {output_dir}/")
        print(f"{'='*60}\n")
        
        # Return summary
        summary = {
            "input_directory": input_dir,
            "output_directory": output_dir,
            "processing_stats": processing_stats,
            "duration_seconds": duration
        }
        
        logger.info(f"Category enrichment completed successfully in {duration:.1f} seconds")
        return summary
        
    except Exception as e:
        logger.error(f"Category enrichment failed: {e}")
        raise


if __name__ == '__main__':
    # Allow running as script
    import sys
    
    input_arg = sys.argv[1] if len(sys.argv) > 1 else None
    output_arg = sys.argv[2] if len(sys.argv) > 2 else None
    run(input_arg, output_arg)

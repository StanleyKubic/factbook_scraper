"""
Multi-Value Splitter for CIA Factbook Data

This module transforms raw country data by splitting multi-valued fields
containing <br> tags into individual values while maintaining order and
creating a uniform data structure.

Author: CIA Factbook Scraper
"""

import json
import os
import re
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple

from utils.logger import get_logger
from utils.config import load_config

# Module-level logger
logger = get_logger(__name__)

# Compiled regex patterns for performance
BR_TAG_PATTERN = re.compile(r'<br\s*/?>', re.IGNORECASE)


def is_multi_valued(data: Optional[str]) -> bool:
    """
    Detect if a data field contains multiple values by checking for <br> tags.
    
    Args:
        data: HTML data field content (can be None or empty)
    
    Returns:
        True if multi-valued (contains ≥1 br tag), False otherwise
    
    Examples:
        >>> is_multi_valued("Value 1<br>Value 2")
        True
        >>> is_multi_valued("Single value")
        False
        >>> is_multi_valued("")
        False
        >>> is_multi_valued(None)
        False
    """
    if not data or not isinstance(data, str):
        return False
    
    # Count br tags using compiled regex
    br_count = len(BR_TAG_PATTERN.findall(data))
    return br_count >= 1


def split_values(data: Optional[str]) -> List[str]:
    """
    Split data field on br tags into individual values.
    
    Args:
        data: HTML data field content
    
    Returns:
        List of individual values, cleaned and ordered
    
    Examples:
        >>> split_values("Value 1<br>Value 2<br>Value 3")
        ['Value 1', 'Value 2', 'Value 3']
        >>> split_values("Value 1<br><br>Value 2")
        ['Value 1', 'Value 2']
        >>> split_values("A<br>B<br/>C<br />D")
        ['A', 'B', 'C', 'D']
        >>> split_values("Single value")
        ['Single value']
        >>> split_values("")
        []
    """
    if not data or not isinstance(data, str):
        return []
    
    # Split on br tags using compiled regex
    split_parts = BR_TAG_PATTERN.split(data)
    
    # Clean and filter values
    cleaned_values = []
    for part in split_parts:
        cleaned = part.strip()
        # Skip empty strings (from consecutive breaks or leading/trailing breaks)
        if cleaned:
            cleaned_values.append(cleaned)
    
    return cleaned_values


def refine_field(field: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform single field from raw to refined format.
    
    Args:
        field: Field object from raw country data
    
    Returns:
        Refined field structure with uniform values array
    
    Examples:
        >>> raw_field = {
        ...     "name": "GDP",
        ...     "database_id": "208", 
        ...     "category": "Economy",
        ...     "data": "$82B (2023)<br>$80B (2022)",
        ...     "subfields": ["2023", "2022"],
        ...     "has_ranking": True
        ... }
        >>> refined = refine_field(raw_field)
        >>> refined['is_multi_valued']
        True
        >>> len(refined['values'])
        2
        >>> refined['values'][0]['value']
        '$82B (2023)'
        >>> refined['values'][0]['order']
        0
    """
    # Extract data field content
    data_content = field.get('data', '')
    
    # Detect if multi-valued
    multi_valued = is_multi_valued(data_content)
    
    # Split values
    if multi_valued:
        values_list = split_values(data_content)
    else:
        # Single value case - put in array for uniformity
        values_list = [data_content] if data_content else []
    
    # Create values array with order
    values_array = [
        {"value": value, "order": i} 
        for i, value in enumerate(values_list)
    ]
    
    # Build refined field structure
    refined_field = {
        "name": field.get('name', ''),
        "database_id": field.get('database_id', ''),
        "category": field.get('category', ''),
        "subfields": field.get('subfields', []),
        "has_ranking": field.get('has_ranking', False),
        "is_multi_valued": multi_valued,
        "values": values_array
    }
    
    return refined_field


def refine_country(country_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform entire country data from raw to refined format.
    
    Args:
        country_data: Raw country data from raw/ directory
    
    Returns:
        Refined country structure with statistics
    """
    start_time = time.time()
    
    # Extract country metadata
    country_slug = country_data.get('country_slug', '')
    scraped_at = country_data.get('scraped_at', '')
    source_url = country_data.get('source_url', '')
    
    # Get raw data
    raw_data = country_data.get('data', {})
    metadata = raw_data.get('metadata', {})
    raw_fields = raw_data.get('fields', [])
    
    # Initialize refined structure
    refined_structure = {
        "country_slug": country_slug,
        "refined_at": datetime.now(timezone.utc).isoformat(),
        "source_file": f"raw/{country_slug}.json",
        "data": {
            "metadata": metadata,
            "fields": []
        },
        "statistics": {
            "total_fields": 0,
            "multi_valued_fields": 0,
            "single_valued_fields": 0
        }
    }
    
    # Process each field
    multi_valued_count = 0
    for field in raw_fields:
        try:
            refined_field = refine_field(field)
            refined_structure["data"]["fields"].append(refined_field)
            
            if refined_field["is_multi_valued"]:
                multi_valued_count += 1
                
        except Exception as e:
            logger.error(f"Error refining field {field.get('name', 'unknown')} in {country_slug}: {e}")
            # Skip problematic field but continue processing
            continue
    
    # Calculate statistics
    total_fields = len(refined_structure["data"]["fields"])
    single_valued_count = total_fields - multi_valued_count
    
    refined_structure["statistics"] = {
        "total_fields": total_fields,
        "multi_valued_fields": multi_valued_count,
        "single_valued_fields": single_valued_count,
        "processing_duration_seconds": time.time() - start_time
    }
    
    logger.debug(f"Refined {country_slug}: {total_fields} fields, {multi_valued_count} multi-valued")
    
    return refined_structure


def save_refined_country(refined_data: Dict[str, Any], output_path: str) -> None:
    """
    Save refined country data to JSON file with atomic write.
    
    Args:
        refined_data: Refined country data
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
            json.dump(refined_data, temp_file, indent=2, ensure_ascii=False)
            temp_path = temp_file.name
        
        # Atomic rename
        os.rename(temp_path, output_path)
        
        logger.debug(f"Saved refined data to {output_path}")
        
    except Exception as e:
        logger.error(f"Failed to save refined data to {output_path}: {e}")
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
    Process all countries with multi-value splitting.
    
    Args:
        input_dir: Path to input directory (raw/ or refined/)
        output_dir: Path to output directory (refined/)
    
    Returns:
        Processing summary with statistics
    """
    start_time = time.time()
    
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
        "total_multi_valued_fields": 0,
        "processing_start": datetime.now(timezone.utc).isoformat(),
        "processing_duration_seconds": 0
    }
    
    logger.info(f"Starting to process {len(country_files)} countries...")
    print(f"\nProcessing countries...")
    
    # Process each country
    for i, country_data in enumerate(country_files, 1):
        try:
            country_slug = country_data.get('country_slug', 'unknown')
            
            # Refine country data
            refined_data = refine_country(country_data)
            
            # Save refined data
            output_path = os.path.join(output_dir, f"{country_slug}.json")
            save_refined_country(refined_data, output_path)
            
            # Update statistics
            processing_stats["successful_countries"] += 1
            processing_stats["total_fields_processed"] += refined_data["statistics"]["total_fields"]
            processing_stats["total_multi_valued_fields"] += refined_data["statistics"]["multi_valued_fields"]
            
            # Progress logging
            if i % 50 == 0 or i == len(country_files):
                print(f"[{i}/{len(country_files)}] Processed {i} countries...")
                
        except Exception as e:
            country_slug = country_data.get('country_slug', 'unknown')
            logger.error(f"Failed to process {country_slug}: {e}")
            processing_stats["failed_countries"].append(country_slug)
            continue
    
    # Calculate final statistics
    processing_stats["processing_duration_seconds"] = time.time() - start_time
    processing_stats["processing_end"] = datetime.now(timezone.utc).isoformat()
    
    # Log summary
    success_rate = processing_stats["successful_countries"] / processing_stats["total_countries"] * 100
    multi_value_rate = processing_stats["total_multi_valued_fields"] / processing_stats["total_fields_processed"] * 100 if processing_stats["total_fields_processed"] > 0 else 0
    
    logger.info(f"Processing complete: {processing_stats['successful_countries']}/{processing_stats['total_countries']} countries ({success_rate:.1f}% success)")
    logger.info(f"Fields processed: {processing_stats['total_fields_processed']}, Multi-valued: {processing_stats['total_multi_valued_fields']} ({multi_value_rate:.1f}%)")
    
    return processing_stats


def count_separator_patterns(raw_data_list: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Count occurrences of different br tag variants across all raw data.
    
    Args:
        raw_data_list: All raw country data
    
    Returns:
        Dictionary with separator pattern counts
    """
    separator_counts = {
        "<br>": 0,
        "<br/>": 0, 
        "<br />": 0,
        "<BR>": 0,
        "<BR/>": 0,
        "<BR />": 0
    }
    
    # Individual regex patterns for each variant
    patterns = {
        "<br>": re.compile(r'<br>', re.IGNORECASE),
        "<br/>": re.compile(r'<br/>', re.IGNORECASE),
        "<br />": re.compile(r'<br\s*/>', re.IGNORECASE)
    }
    
    total_br_count = 0
    
    for country_data in raw_data_list:
        try:
            raw_fields = country_data.get('data', {}).get('fields', [])
            
            for field in raw_fields:
                data_content = field.get('data', '')
                if data_content:
                    # Count each pattern variant
                    for pattern_name, pattern in patterns.items():
                        count = len(pattern.findall(data_content))
                        separator_counts[pattern_name] += count
                    
                    # Total count using main pattern
                    total_br_count += len(BR_TAG_PATTERN.findall(data_content))
                    
        except Exception as e:
            logger.debug(f"Error counting separators in country data: {e}")
            continue
    
    # Normalize counts (some patterns overlap, so we'll use the specific counts)
    logger.debug(f"Found {total_br_count} total br tag occurrences")
    
    return separator_counts


def analyze_multi_value_patterns(refined_data_list: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Analyze multi-value patterns across all refined countries.
    
    Args:
        refined_data_list: All refined country data
    
    Returns:
        Multi-value analysis dictionary
    """
    # Initialize field registry
    field_registry = {}
    total_fields = 0
    total_multi_valued = 0
    
    for country_data in refined_data_list:
        try:
            refined_fields = country_data.get('data', {}).get('fields', [])
            
            for field in refined_fields:
                field_name = field.get('name', 'unknown')
                is_multi_valued = field.get('is_multi_valued', False)
                values_count = len(field.get('values', []))
                
                # Initialize field entry if not exists
                if field_name not in field_registry:
                    field_registry[field_name] = {
                        "countries_analyzed": 0,
                        "multi_valued_count": 0,
                        "single_valued_count": 0,
                        "value_counts": []
                    }
                
                # Update field statistics
                field_registry[field_name]["countries_analyzed"] += 1
                field_registry[field_name]["value_counts"].append(values_count)
                
                if is_multi_valued:
                    field_registry[field_name]["multi_valued_count"] += 1
                    total_multi_valued += 1
                else:
                    field_registry[field_name]["single_valued_count"] += 1
                
                total_fields += 1
                
        except Exception as e:
            logger.debug(f"Error analyzing country data: {e}")
            continue
    
    # Calculate field-level aggregates
    field_analysis = {}
    top_multi_valued_fields = []
    
    for field_name, stats in field_registry.items():
        countries_analyzed = stats["countries_analyzed"]
        multi_valued_count = stats["multi_valued_count"]
        single_valued_count = stats["single_valued_count"]
        value_counts = stats["value_counts"]
        
        # Calculate percentages and averages
        multi_valued_percentage = (multi_valued_count / countries_analyzed * 100) if countries_analyzed > 0 else 0
        avg_value_count = sum(value_counts) / len(value_counts) if value_counts else 0
        min_value_count = min(value_counts) if value_counts else 0
        max_value_count = max(value_counts) if value_counts else 0
        typical_value_count = max(set(value_counts), key=value_counts.count) if value_counts else 0
        
        field_analysis[field_name] = {
            "countries_analyzed": countries_analyzed,
            "multi_valued_count": multi_valued_count,
            "single_valued_count": single_valued_count,
            "multi_valued_percentage": round(multi_valued_percentage, 1),
            "avg_value_count": round(avg_value_count, 1),
            "min_value_count": min_value_count,
            "max_value_count": max_value_count,
            "typical_value_count": typical_value_count
        }
        
        # Add to top multi-valued fields if applicable
        if multi_valued_count > 0:
            top_multi_valued_fields.append({
                "field_name": field_name,
                "multi_valued_percentage": round(multi_valued_percentage, 1),
                "avg_values": round(avg_value_count, 1)
            })
    
    # Sort top multi-valued fields by percentage
    top_multi_valued_fields.sort(key=lambda x: x["multi_valued_percentage"], reverse=True)
    
    # Create final analysis
    analysis = {
        "analyzed_at": datetime.now(timezone.utc).isoformat(),
        "snapshot_date": os.path.basename(get_latest_snapshot()) if refined_data_list else "unknown",
        "total_countries": len(refined_data_list),
        "summary": {
            "total_fields_analyzed": total_fields,
            "multi_valued_occurrences": total_multi_valued,
            "single_valued_occurrences": total_fields - total_multi_valued,
            "multi_value_percentage": round((total_multi_valued / total_fields * 100) if total_fields > 0 else 0, 1)
        },
        "by_field": field_analysis,
        "top_multi_valued_fields": top_multi_valued_fields[:20]  # Top 20
    }
    
    return analysis


def save_analysis_report(analysis: Dict[str, Any], output_path: str) -> None:
    """
    Save multi-value analysis report.
    
    Args:
        analysis: Analysis results
        output_path: Path to save analysis report
    """
    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Write analysis report
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(analysis, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved analysis report to {output_path}")
        
    except Exception as e:
        logger.error(f"Failed to save analysis report to {output_path}: {e}")
        raise


def validate_refined_structure(refined_data: Dict[str, Any]) -> bool:
    """
    Validate refined data structure integrity.
    
    Args:
        refined_data: Refined country data
    
    Returns:
        True if valid, False otherwise
    """
    try:
        # Check required top-level keys
        required_keys = ["country_slug", "data", "statistics"]
        for key in required_keys:
            if key not in refined_data:
                logger.warning(f"Missing required key: {key}")
                return False
        
        # Check data structure
        data = refined_data.get('data', {})
        if 'metadata' not in data or 'fields' not in data:
            logger.warning("Missing metadata or fields in data")
            return False
        
        # Check each field
        fields = data.get('fields', [])
        for field in fields:
            # Check required field keys
            if 'values' not in field:
                logger.warning("Field missing values array")
                return False
            
            # Check values array structure
            values = field.get('values', [])
            for value_item in values:
                if 'value' not in value_item or 'order' not in value_item:
                    logger.warning("Value item missing required keys")
                    return False
            
            # Check is_multi_valued consistency
            is_multi_valued = field.get('is_multi_valued', False)
            values_length = len(values)
            
            if is_multi_valued and values_length < 2:
                logger.warning(f"Multi-valued field has fewer than 2 values: {values_length}")
                return False
            
            if not is_multi_valued and values_length != 1:
                logger.warning(f"Single-valued field has {values_length} values (expected 1)")
                return False
        
        return True
        
    except Exception as e:
        logger.error(f"Error validating refined structure: {e}")
        return False


def run(input_dir: Optional[str] = None, output_dir: Optional[str] = None) -> Dict[str, Any]:
    """
    Main execution - process all countries and generate analysis report.
    
    Args:
        input_dir: Input directory (default: latest snapshot/raw/)
        output_dir: Output directory (default: latest snapshot/refined/)
    
    Returns:
        Processing summary dictionary
    """
    start_time = time.time()
    analysis_path = None
    
    try:
        # Determine directories
        snapshot_dir = None
        if input_dir is None:
            snapshot_dir = get_latest_snapshot()
            input_dir = os.path.join(snapshot_dir, 'raw')
        
        if output_dir is None:
            if snapshot_dir is None:
                snapshot_dir = get_latest_snapshot()
            output_dir = os.path.join(snapshot_dir, 'refined')
        
        logger.info(f"Starting multi-value splitting")
        print(f"\n{'='*60}")
        print(f"    Multi-Value Splitter Started")
        print(f"{'='*60}")
        print(f"Input: {input_dir}")
        print(f"Output: {output_dir}")
        print(f"{'='*60}\n")
        
        # Process all countries
        processing_stats = process_all_countries(input_dir, output_dir)
        
        # Load refined data for analysis (input_dir could be raw or refined)
        if input_dir.endswith('/refined'):
            refined_dir = input_dir
            raw_dir = input_dir.replace('/refined', '/raw')
        else:
            refined_dir = output_dir  # Use output directory for refined data
            raw_dir = input_dir
            
        refined_files = load_country_files(refined_dir)
        raw_files = load_country_files(raw_dir)
        
        # Analyze multi-value patterns
        logger.info("Analyzing multi-value patterns...")
        analysis = analyze_multi_value_patterns(refined_files)
        
        # Count separator patterns
        separator_stats = count_separator_patterns(raw_files)
        analysis["separator_statistics"] = separator_stats
        
        # Save analysis report
        if snapshot_dir:
            analysis_dir = os.path.join(snapshot_dir, 'analysis')
            analysis_path = os.path.join(analysis_dir, 'multi_value_report.json')
            save_analysis_report(analysis, analysis_path)
        
        # Print summary statistics
        duration = time.time() - start_time
        print(f"\n{'='*60}")
        print(f"    Multi-Value Splitting Complete")
        print(f"{'='*60}")
        print(f"Duration: {duration:.1f} seconds")
        print(f"Countries processed: {processing_stats['successful_countries']}/{processing_stats['total_countries']}")
        print(f"Total fields: {processing_stats['total_fields_processed']:,}")
        print(f"Multi-valued fields: {processing_stats['total_multi_valued_fields']:,} ({analysis['summary']['multi_value_percentage']}%)")
        print(f"Single-valued fields: {processing_stats['total_fields_processed'] - processing_stats['total_multi_valued_fields']:,}")
        
        if processing_stats['failed_countries']:
            print(f"\nFailed countries: {len(processing_stats['failed_countries'])}")
            for country in processing_stats['failed_countries'][:5]:
                print(f"  - {country}")
            if len(processing_stats['failed_countries']) > 5:
                print(f"  ... and {len(processing_stats['failed_countries']) - 5} more")
        
        print(f"\nTop Multi-Valued Fields:")
        for i, field in enumerate(analysis['top_multi_valued_fields'][:5], 1):
            print(f"  {i}. {field['field_name']}: {field['multi_valued_percentage']}% (avg {field['avg_values']} values)")
        
        print(f"\nSeparator Statistics:")
        for pattern, count in separator_stats.items():
            if count > 0:
                print(f"  {pattern}: {count:,}")
        
        print(f"\nOutput:")
        print(f"  Refined files: {refined_dir}/")
        if analysis_path:
            print(f"  Analysis report: {analysis_path}")
        print(f"{'='*60}\n")
        
        # Return summary
        summary = {
            "snapshot_directory": snapshot_dir,
            "processing_stats": processing_stats,
            "analysis": analysis,
            "duration_seconds": duration
        }
        
        logger.info(f"Multi-value splitting completed successfully in {duration:.1f} seconds")
        return summary
        
    except Exception as e:
        logger.error(f"Multi-value splitting failed: {e}")
        raise


if __name__ == '__main__':
    # Allow running as script
    import sys
    
    snapshot_arg = sys.argv[1] if len(sys.argv) > 1 else None
    run(snapshot_arg)

"""
Category mapping discovery for CIA Factbook scraper.

This module fetches and parses the category mapping JSON from the CIA Factbook,
extracting database_id to category mappings and saving them for use during scraping.
"""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from scrapers.fetcher import fetch_page_data
from utils.logger import get_logger

# Module-level logger
logger = get_logger(__name__)

# Default URL for category mapping (hash may change with site updates)
DEFAULT_CATEGORY_URL = "https://www.cia.gov/the-world-factbook/page-data/sq/d/2962548448.json"


def fetch_category_mapping(url: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Fetch the category JSON file from CIA Factbook.
    
    Args:
        url: Override default URL for category mapping
        
    Returns:
        Parsed JSON dict or None on failure
    """
    if url is None:
        url = DEFAULT_CATEGORY_URL
    
    logger.info(f"Fetching category mapping from: {url}")
    
    try:
        # Use existing fetcher with retry logic
        category_data = fetch_page_data(url)
        
        if category_data is None:
            logger.error(f"Failed to fetch category mapping from {url}")
            logger.error("This may indicate the URL hash has changed - check CIA Factbook site structure")
            return None
        
        logger.info("Successfully fetched category mapping JSON")
        return category_data
        
    except Exception as e:
        logger.error(f"Error fetching category mapping: {e}")
        return None


def extract_mapping(category_json: Dict[str, Any]) -> Dict[str, str]:
    """
    Extract database_id to category mapping from JSON.
    
    Args:
        category_json: Parsed category JSON from fetch_category_mapping()
        
    Returns:
        Mapping of database_id to category name
    """
    logger.info("Extracting database_id to category mapping")
    
    mapping = {}
    
    try:
        # Navigate to data.allLaunchpadCategory.nodes
        categories = safe_navigate(category_json, "data.allLaunchpadCategory.nodes", [])
        
        if not isinstance(categories, list):
            logger.error("Category structure is not a list")
            return {}
        
        for category in categories:
            if not isinstance(category, dict):
                continue
                
            category_name = category.get("name")
            if not category_name:
                continue
            
            # Extract database_id mapping for each fieldLabel in this category
            field_labels = category.get("fieldLabels", [])
            if isinstance(field_labels, list):
                for field_label in field_labels:
                    if isinstance(field_label, dict):
                        database_id = field_label.get("databaseId")
                        if database_id:
                            mapping[str(database_id)] = category_name
        
        logger.info(f"Extracted mapping for {len(mapping)} fields across {len(categories)} categories")
        return mapping
        
    except Exception as e:
        logger.error(f"Error extracting category mapping: {e}")
        return {}


def extract_category_details(category_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract full category structure with field lists.
    
    NOTE: This function is deprecated and no longer used.
    The category_mapping.json file has been rationalized to only include metadata and mapping.
    
    Args:
        category_json: Parsed category JSON from fetch_category_mapping()
        
    Returns:
        Empty list (function deprecated)
    """
    logger.debug("extract_category_details() called but function is deprecated")
    return []


def save_category_mapping(
    mapping: Dict[str, str], 
    categories_count: int, 
    output_path: str
) -> None:
    """
    Save processed mapping to JSON file.
    
    Args:
        mapping: Database ID to category mapping
        categories_count: Number of categories for metadata
        output_path: Path to save file
    """
    logger.info(f"Saving category mapping to: {output_path}")
    
    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Create rationalized structure with only metadata and mapping
        category_data = {
            "metadata": {
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "source_url": DEFAULT_CATEGORY_URL,
                "total_categories": categories_count,
                "total_fields": len(mapping)
            },
            "mapping": mapping
        }
        
        # Write to file with atomic write
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(category_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Successfully saved rationalized category mapping with {len(mapping)} field mappings")
        
    except Exception as e:
        logger.error(f"Error saving category mapping: {e}")
        raise


def safe_navigate(data: Dict[str, Any], path: str, default: Any = None) -> Any:
    """
    Safely navigate nested dict with dot notation.
    
    Args:
        data: Dictionary to navigate
        path: Dot-notation path (e.g., "data.allLaunchpadCategory.nodes")
        default: Default if not found
        
    Returns:
        Value at path or default
    """
    if not isinstance(data, dict) or not path:
        return default
    
    keys = path.split('.')
    current = data
    
    try:
        for key in keys:
            if not isinstance(current, dict) or key not in current:
                return default
            current = current[key]
        return current
    except (TypeError, KeyError):
        return default


def run() -> Dict[str, Any]:
    """
    Main execution - fetch, parse, save category mapping.
    
    Returns:
        Summary statistics
    """
    logger.info("Starting category mapping discovery")
    start_time = datetime.now(timezone.utc)
    
    try:
        # Fetch category JSON
        category_json = fetch_category_mapping()
        if category_json is None:
            logger.error("Failed to fetch category mapping - aborting")
            return {
                "success": False,
                "error": "fetch_failed",
                "message": "Failed to fetch category mapping from CIA Factbook"
            }
        
        # Extract database_id mapping
        mapping = extract_mapping(category_json)
        if not mapping:
            logger.error("No category mappings extracted - aborting")
            return {
                "success": False,
                "error": "extract_failed", 
                "message": "No category mappings could be extracted from the JSON"
            }
        
        # Count categories from the raw JSON for metadata
        categories = safe_navigate(category_json, "data.allLaunchpadCategory.nodes", [])
        categories_count = len(categories) if isinstance(categories, list) else 0
        
        # Save to file with rationalized structure
        output_path = os.path.join("data", "index", "category_mapping.json")
        save_category_mapping(mapping, categories_count, output_path)
        
        # Return summary
        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        summary = {
            "success": True,
            "categories_count": categories_count,
            "fields_count": len(mapping),
            "output_path": output_path,
            "duration_seconds": duration,
            "fetched_at": start_time.isoformat()
        }
        
        logger.info(f"Category mapping discovery completed: {summary}")
        return summary
        
    except Exception as e:
        logger.error(f"Category mapping discovery failed: {e}")
        return {
            "success": False,
            "error": "unexpected_error",
            "message": str(e)
        }


if __name__ == "__main__":
    # Allow running as module for testing
    summary = run()
    print(f"Category mapping discovery: {summary}")

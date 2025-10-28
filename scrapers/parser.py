"""
Structure parser for CIA Factbook page-data.json files.

This module transforms verbose Gatsby JSON into a clean, simplified structure
while preserving all raw data content and essential metadata.
"""

import re
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Union

from utils.logger import get_logger

# Module-level logger
logger = get_logger(__name__)

# Date parsing patterns
DATE_PATTERNS = [
    r'([A-Za-z]+ \d{1,2}, \d{4})',  # "September 30, 2025"
    r'(\d{1,2} [A-Za-z]+ \d{4})',  # "30 September 2025"
    r'([A-Za-z]+ \d{4})',            # "September 2025"
    r'(\d{4})',                     # "2025"
]


def parse_country_data(
    page_data: Dict[str, Any], 
    source_url: Optional[str] = None
) -> Dict[str, Any]:
    """
    Main entry point - transform verbose Gatsby JSON to clean structure.
    
    Args:
        page_data: Raw page-data.json from fetcher
        source_url: Source URL for traceability
    
    Returns:
        Simplified, filtered country data
    """
    logger.info(f"Starting structure parsing for country data")
    
    if not validate_structure(page_data):
        logger.error("Invalid page-data.json structure")
        return {
            "metadata": {
                "scraped_at": datetime.now(timezone.utc).isoformat(),
                "source_url": source_url,
                "error": "Invalid JSON structure"
            },
            "assets": {},
            "fields": []
        }
    
    try:
        # Extract and simplify metadata
        metadata = extract_metadata(page_data)
        
        # Add extraction metadata
        metadata.update({
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "source_url": source_url
        })
        
        # Extract fields and assets
        fields = extract_fields(page_data)
        assets = extract_assets(page_data)
        
        result = {
            "metadata": metadata,
            "assets": assets,
            "fields": fields
        }
        
        logger.info(f"Successfully parsed country: {metadata.get('name', 'Unknown')}")
        logger.info(f"Processed {len(fields)} fields, extracted {len(assets)} asset types")
        
        return result
        
    except Exception as e:
        logger.error(f"Error parsing country data: {e}")
        return {
            "metadata": {
                "scraped_at": datetime.now(timezone.utc).isoformat(),
                "source_url": source_url,
                "error": str(e)
            },
            "assets": {},
            "fields": []
        }


def extract_metadata(page_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract and simplify country metadata.
    
    Args:
        page_data: Raw page-data.json
    
    Returns:
        Simplified metadata dict
    """
    logger.debug("Extracting country metadata")
    
    # Navigate to result.data.country
    country_data = safe_navigate(page_data, "result.data.country", {})
    
    metadata = {
        "name": country_data.get("name"),
        "region": country_data.get("region"),
        "updated": normalize_date(country_data.get("updated")),
    }
    
    # Count fields from result.data.fields.nodes
    fields_data = safe_navigate(page_data, "result.data.fields.nodes", [])
    metadata["field_count"] = len(fields_data)
    
    logger.debug(f"Metadata extracted: name={metadata['name']}, region={metadata['region']}, "
                f"updated={metadata['updated']}, fields={metadata['field_count']}")
    
    return metadata


def extract_fields(page_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract and filter fields array, removing verbose metadata.
    
    Args:
        page_data: Raw page-data.json
    
    Returns:
        List of filtered field objects
    """
    logger.debug("Extracting fields array")
    
    # Navigate to result.data.fields.nodes
    fields_data = safe_navigate(page_data, "result.data.fields.nodes", [])
    
    if not isinstance(fields_data, list):
        logger.warning("fields.nodes is not a list")
        return []
    
    fields = []
    for i, field in enumerate(fields_data):
        try:
            simplified_field = {
                "name": field.get("name"),
                "data": field.get("data"),  # CRITICAL: Preserve raw content untouched!
                "database_id": extract_database_id(field.get("fieldLabel", [])),
                "subfields": simplify_subfields(field.get("subfields", [])),
                "has_ranking": extract_has_ranking(field.get("fieldLabel", [])),
                "media": simplify_media(field.get("media", []))
            }
            fields.append(simplified_field)
            
        except Exception as e:
            logger.warning(f"Error processing field {i}: {e}")
            continue
    
    logger.debug(f"Extracted {len(fields)} fields successfully")
    return fields


def simplify_subfields(subfields: List[Dict[str, Any]]) -> List[str]:
    """
    Convert subfields array to simple label list.
    
    Args:
        subfields: Raw subfields array
    
    Returns:
        Simple list of subfield labels
    """
    if not isinstance(subfields, list):
        logger.debug("subfields is not a list, returning empty list")
        return []
    
    labels = []
    for subfield in subfields:
        if isinstance(subfield, dict):
            label = subfield.get("label")
            if label:
                labels.append(label)
    
    return labels


def extract_database_id(field_label: List[Dict[str, Any]]) -> Optional[str]:
    """
    Extract database_id from complex fieldLabel structure.
    
    Args:
        field_label: fieldLabel array from field
    
    Returns:
        Database ID as string or None if not found
    """
    if not isinstance(field_label, list) or not field_label:
        return None
    
    try:
        # Get first element (typically only one)
        first_label = field_label[0]
        if isinstance(first_label, dict):
            database_id = first_label.get("databaseId")
            return str(database_id) if database_id is not None else None
    except (IndexError, TypeError):
        return None
    
    return None


def extract_has_ranking(field_label: List[Dict[str, Any]]) -> bool:
    """
    Extract ranking boolean from complex fieldLabel structure.
    
    Args:
        field_label: fieldLabel array from field
    
    Returns:
        True if field has ranking
    """
    if not isinstance(field_label, list) or not field_label:
        return False
    
    try:
        # Get first element (typically only one)
        first_label = field_label[0]
        return bool(first_label.get("rank", False))
    except (IndexError, TypeError):
        return False


def simplify_media(media: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Extract essential media info, discarding verbose Gatsby metadata.
    
    Args:
        media: Raw media array from field
    
    Returns:
        Simplified media objects
    """
    if not isinstance(media, list):
        return []
    
    simplified_media = []
    for media_item in media:
        if not isinstance(media_item, dict):
            continue
        
        simplified = {
            "type": media_item.get("type"),
            "label": media_item.get("label"),
            "alt_text": media_item.get("altText"),
            "caption": media_item.get("caption"),
        }
        
        # Extract URL from localFile.publicURL or similar nested structure
        local_file = media_item.get("localFile", {})
        if isinstance(local_file, dict):
            simplified["url"] = local_file.get("publicURL")
        
        # Only include if we have essential data
        if simplified["type"] and simplified["url"]:
            simplified_media.append(simplified)
    
    return simplified_media


def extract_assets(page_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract asset URLs, discarding verbose gatsbyImageData.
    
    Args:
        page_data: Raw page-data.json
    
    Returns:
        Simplified assets dict
    """
    logger.debug("Extracting assets")
    
    # Navigate to result.data.country
    country_data = safe_navigate(page_data, "result.data.country", {})
    
    assets = {}
    
    # Extract flag
    flag_data = country_data.get("flag", {})
    assets["flag"] = extract_image_asset(flag_data)
    
    # Extract map
    map_data = country_data.get("map", {})
    assets["map"] = extract_image_asset(map_data)
    
    # Extract locator map
    locator_map_data = country_data.get("locatorMap", {})
    assets["locator_map"] = extract_image_asset(locator_map_data)
    
    # Extract images array
    images_data = country_data.get("images", [])
    assets["images"] = extract_images_array(images_data)
    
    logger.debug(f"Extracted assets: flag={bool(assets.get('flag'))}, "
                f"map={bool(assets.get('map'))}, "
                f"locator_map={bool(assets.get('locator_map'))}, "
                f"images={len(assets.get('images', []))}")
    
    return assets


def extract_image_asset(image_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Extract URL and dimensions from gatsbyImageData structure.
    
    Args:
        image_data: Raw image asset from Gatsby
    
    Returns:
        Simplified image asset with url, width, height
    """
    if not isinstance(image_data, dict):
        return None
    
    # Navigate through gatsbyImageData -> images -> fallback -> src
    gatsby_data = safe_navigate(image_data, "childImageSharp.gatsbyImageData")
    if not gatsby_data:
        return None
    
    # Extract URL from nested structure
    url = safe_navigate(gatsby_data, "images.fallback.src")
    width = safe_navigate(gatsby_data, "width")
    height = safe_navigate(gatsby_data, "height")
    
    if url:
        return {
            "url": url,
            "width": width,
            "height": height
        }
    
    return None


def extract_images_array(images_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Extract simplified images array.
    
    Args:
        images_data: Raw images array from country
    
    Returns:
        Simplified images list
    """
    if not isinstance(images_data, list):
        return []
    
    images = []
    for image_item in images_data:
        if not isinstance(image_item, dict):
            continue
        
        simplified = {
            "alt_text": image_item.get("altText"),
            "caption": image_item.get("caption"),
        }
        
        # Extract URL from gatsbyImageData structure
        gatsby_data = safe_navigate(image_item, "childImageSharp.gatsbyImageData")
        if gatsby_data:
            simplified["url"] = safe_navigate(gatsby_data, "images.fallback.src")
            simplified["width"] = safe_navigate(gatsby_data, "width")
            simplified["height"] = safe_navigate(gatsby_data, "height")
        
        if simplified.get("url"):
            images.append(simplified)
    
    return images


def safe_navigate(data: Dict[str, Any], path: str, default: Any = None) -> Any:
    """
    Safely navigate nested dict with dot notation.
    
    Args:
        data: Dictionary to navigate
        path: Dot-notation path (e.g., "result.data.country.name")
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


def validate_structure(page_data: Dict[str, Any]) -> bool:
    """
    Validate input has expected Gatsby structure.
    
    Args:
        page_data: Raw page-data.json
    
    Returns:
        True if valid
    """
    if not isinstance(page_data, dict):
        logger.debug("page_data is not a dict")
        return False
    
    # Check for "result" key
    if "result" not in page_data:
        logger.debug("Missing 'result' key")
        return False
    
    result = page_data["result"]
    if not isinstance(result, dict):
        logger.debug("result is not a dict")
        return False
    
    # Check for "data" key in result
    if "data" not in result:
        logger.debug("Missing 'result.data' key")
        return False
    
    data_content = result["data"]
    if not isinstance(data_content, dict):
        logger.debug("result.data is not a dict")
        return False
    
    # Check for country or fields key
    if "country" not in data_content and "fields" not in data_content:
        logger.debug("Neither 'country' nor 'fields' found in result.data")
        return False
    
    return True


def normalize_date(date_str: Optional[str]) -> Optional[str]:
    """
    Normalize various date formats to ISO format (YYYY-MM-DD).
    
    Args:
        date_str: Date string in various formats
    
    Returns:
        Normalized date string or None
    """
    if not date_str or not isinstance(date_str, str):
        return None
    
    date_str = date_str.strip()
    
    # Try each pattern
    for pattern in DATE_PATTERNS:
        match = re.search(pattern, date_str)
        if match:
            matched = match.group(1)
            try:
                # Try to parse the matched date
                if re.match(r'\d{4}', matched):  # Just year
                    return f"{matched}-01-01"
                elif re.match(r'[A-Za-z]+ \d{4}', matched):  # Month Year
                    return datetime.strptime(matched, "%B %Y").strftime("%Y-%m-01")
                elif re.match(r'\d{1,2} [A-Za-z]+ \d{4}', matched):  # Day Month Year
                    return datetime.strptime(matched, "%d %B %Y").strftime("%Y-%m-%d")
                elif re.match(r'[A-Za-z]+ \d{1,2}, \d{4}', matched):  # Month Day, Year
                    return datetime.strptime(matched, "%B %d, %Y").strftime("%Y-%m-%d")
            except ValueError:
                logger.debug(f"Failed to parse date: {matched}")
                continue
    
    logger.debug(f"Could not normalize date: {date_str}")
    return date_str  # Return original if normalization fails

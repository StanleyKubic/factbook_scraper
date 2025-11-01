"""
Year Extractor for CIA Factbook Data

This module extracts year information from value strings using regex patterns
to identify years in formats like "(2024)" or "(2024 est.)".

Enhanced with heuristics to avoid extracting years from descriptive text
that contains multiple historical dates or is too long to be a simple data point.

Author: CIA Factbook Scraper
"""

import re
from typing import Optional

# Compiled regex patterns for performance
# Order matters - more specific patterns first
YEAR_PATTERNS = [
    re.compile(r'\((\d{4}) est\.\)'),  # (2024 est.)
    re.compile(r'\((\d{4})\)')         # (2024)
]

# Configuration for heuristics
MAX_VALUE_LENGTH = 120  # Maximum character length for simple data points
MAX_YEARS_IN_VALUE = 1  # Maximum number of years to consider valid


def should_extract_year(value: str, max_length: int = MAX_VALUE_LENGTH, max_years: int = MAX_YEARS_IN_VALUE) -> bool:
    """
    Determine if year should be extracted from this value using heuristics.
    
    Args:
        value: String value that may contain a year
        max_length: Maximum character length for simple data points
        max_years: Maximum number of years allowed for extraction
    
    Returns:
        True if year extraction is appropriate, False otherwise
    
    Examples:
        >>> should_extract_year("0.6% of GDP (2024 est.)")
        True
        >>> should_extract_year("previous: Independence Day, 19 August (1919); under the Taliban Government, 15 August (2022) is declared a national holiday, marking anniversary of victory of the Afghan jihad")
        False
        >>> should_extract_year("long descriptive text with many historical references and context about what happened in (1919) and then later in (2022) and other years")
        False
    """
    if not value or not isinstance(value, str):
        return False
    
    # Count all year patterns in value first
    total_years_found = 0
    for pattern in YEAR_PATTERNS:
        matches = pattern.findall(value)
        total_years_found += len(matches)
    
    # If too many years found, it's likely descriptive text
    if total_years_found > max_years:
        return False
    
    # Check length heuristic (only apply if exactly one year found)
    if total_years_found == 1 and len(value) > max_length:
        return False
    
    return True


def extract_year_smart(value: str) -> Optional[str]:
    """
    Enhanced year extraction with heuristics to avoid descriptive text.
    
    Args:
        value: String value that may contain a year
    
    Returns:
        Year as string if appropriate, None otherwise
    
    Examples:
        >>> extract_year_smart("0.6% of GDP (2024 est.)")
        '2024'
        >>> extract_year_smart("previous: Independence Day, 19 August (1919); under the Taliban Government, 15 August (2022) is declared a national holiday")
        None
        >>> extract_year_smart("Data from (2022)")
        '2022'
    """
    if not should_extract_year(value):
        return None
    
    # Use existing extraction logic for valid cases
    return extract_year(value)


def extract_year(value: str) -> Optional[str]:
    """
    Extract year from a value string using regex patterns.
    
    Args:
        value: String value that may contain a year
    
    Returns:
        Year as string if found, None otherwise
    
    Examples:
        >>> extract_year("0.6% of GDP (2024 est.)")
        '2024'
        >>> extract_year("0.5% of GDP (2023 est.)")
        '2023'
        >>> extract_year("Single value without year")
        None
        >>> extract_year("Data from (2022)")
        '2022'
    """
    if not value or not isinstance(value, str):
        return None
    
    # Try each pattern in order
    for pattern in YEAR_PATTERNS:
        match = pattern.search(value)
        if match:
            return match.group(1)
    
    return None


def extract_years_from_values(values_list: list) -> list:
    """
    Extract years from a list of value objects using smart extraction.
    
    Args:
        values_list: List of value dictionaries with 'value' key
    
    Returns:
        List of value dictionaries with 'year' field added where appropriate
    
    Examples:
        >>> values = [
        ...     {"value": "0.6% of GDP (2024 est.)", "order": 0},
        ...     {"value": "previous: Independence Day, 19 August (1919); under the Taliban Government, 15 August (2022) is declared a national holiday", "order": 1},
        ...     {"value": "No year here", "order": 2}
        ... ]
        >>> result = extract_years_from_values(values)
        >>> result[0]["year"]
        '2024'
        >>> "year" not in result[1]
        True
        >>> "year" not in result[2]
        True
    """
    if not values_list:
        return []
    
    result = []
    for value_obj in values_list:
        # Create a copy to avoid modifying original
        enhanced_value = value_obj.copy()
        
        # Extract year from value field using smart extraction
        year = extract_year_smart(enhanced_value.get('value', ''))
        if year:
            enhanced_value['year'] = year
        
        result.append(enhanced_value)
    
    return result


def extract_years_from_key_value_pairs(values_list: list) -> list:
    """
    Extract years from a list of key-value pair objects using smart extraction.
    
    Args:
        values_list: List of dictionaries with 'key' and 'value' keys
    
    Returns:
        List of key-value dictionaries with 'year' field added where found
    """
    if not values_list:
        return []
    
    result = []
    for kv_obj in values_list:
        # Create a copy to avoid modifying original
        enhanced_kv = kv_obj.copy()
        
        # Extract year from value field using smart extraction
        year = extract_year_smart(enhanced_kv.get('value', ''))
        if year:
            enhanced_kv['year'] = year
        
        result.append(enhanced_kv)
    
    return result


def extract_years_from_key_with_sub_values(key_with_sub_values: dict) -> dict:
    """
    Extract years from a key with sub-values structure using smart extraction.
    
    Args:
        key_with_sub_values: Dictionary with 'key' and 'sub_values' keys
    
    Returns:
        Enhanced dictionary with 'year' field added to sub-values where found
    """
    if not key_with_sub_values:
        return {}
    
    result = key_with_sub_values.copy()
    
    # Extract years from sub_values
    sub_values = result.get('sub_values', [])
    if sub_values:
        enhanced_sub_values = []
        for sub_value in sub_values:
            # Handle both string and dict sub_value formats
            if isinstance(sub_value, str):
                year = extract_year_smart(sub_value)
                if year:
                    enhanced_sub_values.append({
                        'value': sub_value,
                        'year': year
                    })
                else:
                    enhanced_sub_values.append({
                        'value': sub_value
                    })
            elif isinstance(sub_value, dict):
                value_text = sub_value.get('value', '')
                year = extract_year_smart(value_text)
                enhanced_sub_value = sub_value.copy()
                if year:
                    enhanced_sub_value['year'] = year
                enhanced_sub_values.append(enhanced_sub_value)
            else:
                enhanced_sub_values.append(sub_value)
        
        result['sub_values'] = enhanced_sub_values
    
    return result


def test_year_extraction():
    """
    Test function to verify year extraction works correctly with heuristics.
    """
    test_cases = [
        # Valid cases that should extract years
        ("0.6% of GDP (2024 est.)", "2024"),
        ("0.5% of GDP (2023 est.)", "2023"),
        ("Data from (2022)", "2022"),
        ("12.8% (2024 est.) (male 2,570,596/female 3,461,743)", "2024"),
        
        # Invalid cases that should NOT extract years (descriptive text)
        ("previous: Independence Day, 19 August (1919); under the Taliban Government, 15 August (2022) is declared a national holiday, marking the anniversary of the victory of the Afghan jihad", None),
        ("long descriptive text with many historical references and context about what happened in (1919) and then later in (2022) and other years", None),
        
        # Edge cases
        ("No year here", None),
        ("", None),
        (None, None),
        ("Multiple years (2023) and (2024 est.)", "2024"),  # Should extract first valid one
        ("Short text (2024)", "2024"),  # Should extract - under length limit
    ]
    
    print("Testing enhanced year extraction:")
    for i, (input_val, expected) in enumerate(test_cases, 1):
        result = extract_year_smart(input_val)
        status = "✓" if result == expected else "✗"
        print(f"  {i}. {status} '{input_val[:50]}...' -> '{result}' (expected: '{expected}')")


if __name__ == '__main__':
    # Run tests when executed directly
    test_year_extraction()

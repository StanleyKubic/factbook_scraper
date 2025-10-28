"""
HTTP fetcher utility for CIA Factbook page-data.json files.

This module provides reliable fetching with retry logic, rate limiting,
and error handling specifically designed for the CIA Factbook scraper.
"""

import time
import json
from typing import Dict, Any, Optional, List, Union
import requests

from utils.config import load_config
from utils.logger import get_logger
from utils.http_client import HTTPClient

# Module-level constants
DEFAULT_TIMEOUT = 30
DEFAULT_RETRIES = 3
DEFAULT_DELAY = 2
USER_AGENT = "CIA-Factbook-Scraper/1.0"

# Module-level logger
logger = get_logger(__name__)

# Module-level HTTP session for connection pooling
_session = None


def _get_session() -> HTTPClient:
    """Get or create the HTTP session."""
    global _session
    if _session is None:
        # Load the full app config, not the simplified dict
        from utils.config import load_config as load_app_config
        app_config = load_app_config()
        _session = HTTPClient(
            timeout=app_config.scraping.request_timeout,
            retry_attempts=app_config.scraping.retry_attempts,
            retry_delay=app_config.scraping.retry_delay,
            rate_limit_delay=app_config.scraping.rate_limit_delay
        )
        # Override user agent to match requirements
        _session.session.headers.update({'User-Agent': USER_AGENT})
    return _session


def load_config() -> Dict[str, Any]:
    """
    Load fetcher configuration from config.yaml.
    
    Returns:
        Configuration dictionary with fetcher settings
    """
    from utils.config import load_config as load_app_config
    
    config = load_app_config()
    return {
        'retry_attempts': config.scraping.retry_attempts,
        'retry_delay': config.scraping.retry_delay,
        'request_timeout': config.scraping.request_timeout,
        'rate_limit_delay': config.scraping.rate_limit_delay
    }


def get_retry_delay(attempt: int, base_delay: float) -> float:
    """
    Calculate exponential backoff delay.
    
    Args:
        attempt: Current attempt number (0-indexed)
        base_delay: Base delay in seconds
    
    Returns:
        Delay in seconds
    """
    return base_delay * (2 ** attempt)


def validate_json_structure(data: Dict[str, Any]) -> bool:
    """
    Validate that fetched JSON has expected structure.
    
    Args:
        data: Parsed JSON data
    
    Returns:
        True if structure is valid
    """
    if not isinstance(data, dict):
        return False
    
    # Check for "result" key
    if "result" not in data:
        return False
    
    result = data["result"]
    if not isinstance(result, dict):
        return False
    
    # Check for "data" key in result
    if "data" not in result:
        return False
    
    data_content = result["data"]
    if not isinstance(data_content, dict):
        return False
    
    # Check for country or fields key
    if "country" not in data_content and "fields" not in data_content:
        return False
    
    return True


def _classify_error(response: Optional[requests.Response] = None, 
                   exception: Optional[Exception] = None) -> str:
    """
    Classify the type of error for appropriate handling.
    
    Args:
        response: HTTP response object (if available)
        exception: Exception that occurred (if available)
    
    Returns:
        Error classification string
    """
    if response is not None:
        status_code = response.status_code
        
        if status_code == 404:
            return "not_found"
        elif 500 <= status_code <= 599:
            return "server_error"
        elif 400 <= status_code <= 499:
            return "client_error"
    
    if exception is not None:
        if isinstance(exception, requests.exceptions.Timeout):
            return "timeout"
        elif isinstance(exception, requests.exceptions.ConnectionError):
            return "network_error"
        elif isinstance(exception, json.JSONDecodeError):
            return "json_parse_error"
        elif isinstance(exception, requests.exceptions.HTTPError):
            # Check if it's a 404 error from the exception
            if hasattr(exception, 'response') and exception.response is not None:
                status_code = exception.response.status_code
                if status_code == 404:
                    return "not_found"
                elif 500 <= status_code <= 599:
                    return "server_error"
                elif 400 <= status_code <= 499:
                    return "client_error"
            # Fall back to client error for HTTP errors
            return "client_error"
    
    return "unknown"


def fetch_page_data(
    url: str,
    timeout: Optional[int] = None,
    retries: Optional[int] = None,
    delay: Optional[float] = None
) -> Optional[Dict[str, Any]]:
    """
    Fetch single page-data.json URL with retry logic.
    
    Args:
        url: Full URL to page-data.json
        timeout: Request timeout (default from config)
        retries: Retry attempts (default from config)
        delay: Rate limit delay (default from config)
    
    Returns:
        Parsed JSON as dict, or None if failed
    """
    config = load_config()
    
    # Use provided values or defaults from config
    request_timeout = timeout or config['request_timeout']
    max_retries = retries or config['retry_attempts']
    base_delay = delay or config['retry_delay']
    
    logger.info(f"Starting fetch for URL: {url}")
    start_time = time.time()
    
    session = _get_session()
    
    for attempt in range(max_retries + 1):  # +1 for initial attempt
        try:
            logger.debug(f"Attempt {attempt + 1}/{max_retries + 1} for {url}")
            
            # Make HTTP GET request
            response_text = session.fetch(url)
            
            # Parse JSON
            try:
                data = json.loads(response_text)
            except json.JSONDecodeError as e:
                error_class = _classify_error(exception=e)
                logger.error(f"JSON parse error for {url}: {e}")
                
                if error_class == "json_parse_error":
                    # Don't retry JSON parse errors
                    return None
                continue
            
            # Validate structure
            if not validate_json_structure(data):
                logger.error(f"Invalid JSON structure for {url}")
                return None
            
            duration = time.time() - start_time
            logger.info(f"Successfully fetched {url} in {duration:.2f}s")
            
            # Apply rate limit delay before returning
            if attempt < max_retries:  # Don't delay on final attempt
                rate_delay = config['rate_limit_delay']
                if rate_delay > 0:
                    logger.debug(f"Rate limiting: sleeping for {rate_delay}s")
                    time.sleep(rate_delay)
            
            return data
            
        except requests.exceptions.RequestException as e:
            error_class = _classify_error(exception=e)
            
            # Handle different error types
            if error_class == "not_found":
                logger.info(f"Resource not found (404) for {url} - this is normal for some URLs")
                return None
            elif error_class in ["timeout", "network_error", "server_error"]:
                if attempt < max_retries:
                    retry_delay = get_retry_delay(attempt, base_delay)
                    logger.warning(f"{error_class} for {url}, retrying in {retry_delay}s (attempt {attempt + 1}/{max_retries + 1})")
                    time.sleep(retry_delay)
                    continue
                else:
                    logger.error(f"Max retries exceeded for {url} due to {error_class}")
            elif error_class == "client_error":
                logger.error(f"Client error for {url}: {e}")
                return None
            else:
                logger.error(f"Unknown error for {url}: {e}")
                if attempt < max_retries:
                    retry_delay = get_retry_delay(attempt, base_delay)
                    time.sleep(retry_delay)
                    continue
                else:
                    logger.error(f"Max retries exceeded for {url} due to unknown error")
        
        except Exception as e:
            logger.error(f"Unexpected error for {url}: {e}")
            if attempt < max_retries:
                retry_delay = get_retry_delay(attempt, base_delay)
                time.sleep(retry_delay)
                continue
    
    duration = time.time() - start_time
    logger.error(f"Failed to fetch {url} after {max_retries + 1} attempts in {duration:.2f}s")
    return None


def fetch_multiple(
    urls: List[str],
    config_overrides: Optional[Dict[str, Any]] = None
) -> Dict[str, Optional[Dict[str, Any]]]:
    """
    Fetch multiple URLs sequentially with rate limiting.
    
    Args:
        urls: List of URLs to fetch
        config_overrides: Override default config
    
    Returns:
        Mapping of URL to result (dict or None)
    """
    logger.info(f"Starting batch fetch for {len(urls)} URLs")
    start_time = time.time()
    
    # Apply config overrides if provided
    if config_overrides:
        original_config = load_config()
        config = {**original_config, **config_overrides}
    else:
        config = load_config()
    
    results: Dict[str, Optional[Dict[str, Any]]] = {}
    
    for i, url in enumerate(urls):
        logger.debug(f"Processing URL {i + 1}/{len(urls)}: {url}")
        
        result = fetch_page_data(
            url,
            timeout=config.get('request_timeout'),
            retries=config.get('retry_attempts'),
            delay=config.get('retry_delay')
        )
        
        results[url] = result
        
        # Rate limiting is already handled by fetch_page_data
        # but we add a small additional delay between URLs if specified
        if i < len(urls) - 1:  # Don't delay after last URL
            additional_delay = config.get('rate_limit_delay', 0)
            if additional_delay > 0:
                logger.debug(f"Additional delay between URLs: {additional_delay}s")
                time.sleep(additional_delay)
    
    duration = time.time() - start_time
    successful_count = sum(1 for result in results.values() if result is not None)
    logger.info(f"Batch fetch completed: {successful_count}/{len(urls)} successful in {duration:.2f}s")
    
    return results

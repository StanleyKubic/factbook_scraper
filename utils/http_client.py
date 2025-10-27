"""
HTTP client utilities with retry logic for the CIA Factbook scraper.
"""

import time
import requests
from typing import Optional, Dict, Any
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from utils.logger import get_logger

logger = get_logger(__name__)


class HTTPClient:
    """
    HTTP client with retry logic and error handling.
    """
    
    def __init__(
        self,
        timeout: int = 30,
        retry_attempts: int = 3,
        retry_delay: int = 2,
        rate_limit_delay: int = 1
    ):
        """
        Initialize HTTP client.
        
        Args:
            timeout: Request timeout in seconds
            retry_attempts: Number of retry attempts
            retry_delay: Base delay between retries in seconds
            rate_limit_delay: Delay between requests in seconds
        """
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay
        self.rate_limit_delay = rate_limit_delay
        self.last_request_time = 0
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=retry_attempts,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
            backoff_factor=retry_delay,
            raise_on_status=False
        )
        
        # Create session with retry strategy
        self.session = requests.Session()
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Set default headers
        self.session.headers.update({
            'User-Agent': 'CIA Factbook Scraper (https://github.com/StanleyKubic/factbook_scraper)',
            'Accept': 'text/xml,application/xml,application/xhtml+xml,text/html;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        })
    
    def _respect_rate_limit(self):
        """Respect rate limiting by waiting if necessary."""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        
        if time_since_last_request < self.rate_limit_delay:
            sleep_time = self.rate_limit_delay - time_since_last_request
            logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def fetch(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Fetch content from URL with retry logic.
        
        Args:
            url: URL to fetch
            headers: Additional headers (optional)
            params: URL parameters (optional)
        
        Returns:
            Response content as string
        
        Raises:
            requests.RequestException: If request fails after all retries
        """
        self._respect_rate_limit()
        
        logger.info(f"Fetching URL: {url}")
        
        # Merge headers
        request_headers = self.session.headers.copy()
        if headers:
            request_headers.update(headers)
        
        try:
            response = self.session.get(
                url,
                headers=request_headers,
                params=params,
                timeout=self.timeout
            )
            
            response.raise_for_status()
            
            logger.info(f"Successfully fetched {url} ({len(response.content)} bytes)")
            return response.text
            
        except requests.exceptions.Timeout:
            logger.error(f"Timeout while fetching {url}")
            raise
        except requests.exceptions.ConnectionError:
            logger.error(f"Connection error while fetching {url}")
            raise
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error {e.response.status_code} while fetching {url}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Request exception while fetching {url}: {e}")
            raise
    
    def close(self):
        """Close the HTTP session."""
        self.session.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

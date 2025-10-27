"""
Sitemap parser for CIA Factbook scraper.

This module fetches and parses CIA Factbook sitemap to extract
all country URLs and categorizes them by type.
"""

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any
from xml.etree import ElementTree as ET

import xmltodict
from pydantic import BaseModel, Field, field_validator

from utils.config import load_config
from utils.logger import setup_logger, get_logger
from utils.http_client import HTTPClient


class CountryURLs(BaseModel):
    """Model for country URLs with categorization."""
    main: Optional[str] = None
    factsheet: Optional[str] = None
    images: Optional[str] = None
    flag: Optional[str] = None
    map: Optional[str] = None
    locator_map: Optional[str] = None
    other: List[str] = Field(default_factory=list)

    @field_validator('other')
    @classmethod
    def validate_other(cls, v):
        """Ensure other URLs is always a list."""
        return v if v is not None else []


class CountryInfo(BaseModel):
    """Model for country information."""
    slug: str
    urls: CountryURLs


class SitemapMetadata(BaseModel):
    """Model for sitemap metadata."""
    scraped_at: str
    sitemap_url: str
    total_countries: int
    total_urls: int
    url_types: Dict[str, int] = Field(default_factory=dict)


class SitemapResult(BaseModel):
    """Model for complete sitemap parsing result."""
    metadata: SitemapMetadata
    countries: List[CountryInfo]


class SitemapParser:
    """
    Main class for parsing CIA Factbook sitemap.
    """
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """
        Initialize sitemap parser.
        
        Args:
            config_path: Path to configuration file
        """
        self.config = load_config(config_path)
        self.logger = get_logger(__name__)
        
        # Setup logger based on config
        setup_logger(
            log_level=self.config.logging.log_level,
            log_to_file=self.config.logging.log_to_file,
            log_to_console=self.config.logging.log_to_console
        )
        
        # Initialize HTTP client
        self.http_client = HTTPClient(
            timeout=self.config.scraping.request_timeout,
            retry_attempts=self.config.scraping.retry_attempts,
            retry_delay=self.config.scraping.retry_delay,
            rate_limit_delay=self.config.scraping.rate_limit_delay
        )
    
    def fetch_sitemap(self, url: str) -> str:
        """
        Download sitemap XML from configured URL.
        
        Args:
            url: Sitemap URL from config
        
        Returns:
            Raw XML content
        
        Raises:
            requests.RequestException: If sitemap cannot be fetched after retries
        """
        self.logger.info(f"Fetching sitemap from: {url}")
        
        try:
            xml_content = self.http_client.fetch(url)
            self.logger.info(f"Successfully fetched sitemap ({len(xml_content)} characters)")
            return xml_content
            
        except Exception as e:
            self.logger.error(f"Failed to fetch sitemap after {self.config.scraping.retry_attempts} attempts: {e}")
            raise
    
    def parse_sitemap_xml(self, xml_content: str) -> List[str]:
        """
        Parse XML and extract all URLs.
        
        Args:
            xml_content: Raw XML string
        
        Returns:
            List of all URLs found in sitemap
        
        Raises:
            ValueError: If XML parsing fails
        """
        self.logger.info("Parsing sitemap XML")
        
        try:
            # Parse XML using xmltodict for better handling of complex structures
            parsed_dict = xmltodict.parse(xml_content)
            
            urls = []
            
            # Handle different sitemap structures
            if 'sitemapindex' in parsed_dict:
                # Sitemap index - contains nested sitemaps
                sitemaps = parsed_dict['sitemapindex'].get('sitemap', [])
                if not isinstance(sitemaps, list):
                    sitemaps = [sitemaps]
                
                for sitemap in sitemaps:
                    if 'loc' in sitemap:
                        urls.append(sitemap['loc'])
                        
            elif 'urlset' in parsed_dict:
                # URL set - contains actual URLs
                url_elements = parsed_dict['urlset'].get('url', [])
                if not isinstance(url_elements, list):
                    url_elements = [url_elements]
                
                for url_elem in url_elements:
                    if 'loc' in url_elem:
                        urls.append(url_elem['loc'])
            
            # Fallback to ElementTree if xmltodict doesn't work
            if not urls:
                self.logger.warning("xmltodict parsing yielded no URLs, trying ElementTree")
                root = ET.fromstring(xml_content)
                
                # Try both namespace approaches
                namespaces = {
                    'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9',
                    'sitemaps': 'http://www.sitemaps.org/schemas/sitemap/0.9'
                }
                
                for ns in namespaces.values():
                    elements = root.findall(f".//{ns}url/{ns}loc")
                    urls.extend([elem.text for elem in elements if elem.text])
                
                # Try without namespace
                if not urls:
                    elements = root.findall(".//url/loc")
                    urls.extend([elem.text for elem in elements if elem.text])
            
            self.logger.info(f"Extracted {len(urls)} URLs from sitemap")
            
            # Log first few URLs for debugging
            if urls:
                sample_urls = urls[:5]
                self.logger.debug(f"Sample URLs: {sample_urls}")
            
            return urls
            
        except ET.ParseError as e:
            self.logger.error(f"XML parsing error: {e}")
            raise ValueError(f"Invalid XML format: {e}")
        except Exception as e:
            self.logger.error(f"Error parsing sitemap XML: {e}")
            raise ValueError(f"Failed to parse sitemap: {e}")
    
    def extract_country_urls(self, urls: List[str]) -> Dict[str, CountryURLs]:
        """
        Filter and categorize country-related URLs.
        
        Args:
            urls: All URLs from sitemap
        
        Returns:
            Dictionary mapping country slugs to their URLs
        """
        self.logger.info("Extracting country URLs")
        
        # Pattern to match country URLs: /the-world-factbook/countries/{slug}
        country_pattern = re.compile(r'/the-world-factbook/countries/([^/]+)(?:/.*)?$')
        
        # Patterns for different URL types
        url_patterns = {
            'main': re.compile(r'^/the-world-factbook/countries/([^/]+)(?:/)?$'),
            'factsheet': re.compile(r'^/the-world-factbook/countries/([^/]+)/factsheets?(?:/)?$'),
            'images': re.compile(r'^/the-world-factbook/countries/([^/]+)/images?(?:/)?$'),
            'flag': re.compile(r'^/the-world-factbook/countries/([^/]+)/flag?(?:/)?$'),
            'map': re.compile(r'^/the-world-factbook/countries/([^/]+)/map?(?:/)?$'),
            'locator_map': re.compile(r'^/the-world-factbook/countries/([^/]+)/locator-map?(?:/)?$'),
        }
        
        countries: Dict[str, CountryURLs] = {}
        total_country_urls = 0
        
        for url in urls:
            # Extract just the path part
            if url.startswith('http'):
                from urllib.parse import urlparse
                path = urlparse(url).path
            else:
                path = url
            
            # Check if this is a country URL
            match = country_pattern.match(path)
            if not match:
                continue
            
            slug = match.group(1)
            total_country_urls += 1
            
            # Debug: log first few URL processing
            if len(countries) <= 3:
                self.logger.debug(f"Processing URL: '{url}' -> path: '{path}'")
            
            # Debug: log first few matches
            if total_country_urls <= 5:
                self.logger.debug(f"Country URL match: path='{path}', slug='{slug}'")
            
            # Initialize country entry if not exists
            if slug not in countries:
                countries[slug] = CountryURLs()
            
            # Categorize the URL
            categorized = False
            for url_type, pattern in url_patterns.items():
                type_match = pattern.match(path)
                if type_match:
                    # Verify slug consistency
                    if type_match.group(1) == slug:
                        setattr(countries[slug], url_type, path)
                        categorized = True
                        break
            
            # If not categorized into specific types, add to 'other'
            if not categorized:
                if path not in countries[slug].other:
                    countries[slug].other.append(path)
        
        self.logger.info(f"Found {len(countries)} countries with {total_country_urls} total URLs")
        
        # Log URL type distribution
        type_counts = {'main': 0, 'factsheet': 0, 'images': 0, 'flag': 0, 'map': 0, 'locator_map': 0, 'other': 0}
        for country_urls in countries.values():
            for url_type in type_counts:
                if url_type == 'other':
                    type_counts[url_type] += len(country_urls.other)
                elif getattr(country_urls, url_type):
                    type_counts[url_type] += 1
        
        self.logger.info(f"URL type distribution: {type_counts}")
        
        return countries
    
    def save_countries_index(self, countries_data: Dict[str, CountryURLs], output_path: str) -> None:
        """
        Save discovered countries to JSON file.
        
        Args:
            countries_data: Country URLs dictionary
            output_path: Path to output file
        """
        self.logger.info(f"Saving countries index to: {output_path}")
        
        # Ensure output directory exists
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Create metadata
        metadata = SitemapMetadata(
            scraped_at=datetime.now(timezone.utc).isoformat(),
            sitemap_url=self.config.sitemap_url,
            total_countries=len(countries_data),
            total_urls=sum(
                1  # main
                + (1 if urls.factsheet else 0)
                + (1 if urls.images else 0)
                + (1 if urls.flag else 0)
                + (1 if urls.map else 0)
                + (1 if urls.locator_map else 0)
                + len(urls.other)
                for urls in countries_data.values()
            ),
            url_types={}
        )
        
        # Calculate URL type distribution
        type_counts = {'main': 0, 'factsheet': 0, 'images': 0, 'flag': 0, 'map': 0, 'locator_map': 0, 'other': 0}
        for country_urls in countries_data.values():
            for url_type in type_counts:
                if url_type == 'other':
                    type_counts[url_type] += len(country_urls.other)
                elif getattr(country_urls, url_type):
                    type_counts[url_type] += 1
        
        metadata.url_types = type_counts
        
        # Create country list (sorted alphabetically by slug)
        country_list = []
        for slug in sorted(countries_data.keys()):
            country_info = CountryInfo(
                slug=slug,
                urls=countries_data[slug]
            )
            country_list.append(country_info)
        
        # Create result
        result = SitemapResult(
            metadata=metadata,
            countries=country_list
        )
        
        # Save to file
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result.model_dump(), f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"Successfully saved {len(countries_data)} countries to {output_path}")
    
    def run(self) -> Dict[str, Any]:
        """
        Main execution function orchestrating the workflow.
        
        Returns:
            Dictionary with summary statistics
        """
        self.logger.info("Starting sitemap parser")
        
        try:
            # Fetch sitemap XML
            xml_content = self.fetch_sitemap(self.config.sitemap_url)
            
            # Parse XML to extract URLs
            urls = self.parse_sitemap_xml(xml_content)
            
            # Extract and categorize country URLs
            countries = self.extract_country_urls(urls)
            
            # Save to data/index/countries.json
            output_path = "data/index/countries.json"
            self.save_countries_index(countries, output_path)
            
            # Return statistics
            stats = {
                "total_countries": len(countries),
                "total_urls": sum(
                    1  # main
                    + (1 if urls.factsheet else 0)
                    + (1 if urls.images else 0)
                    + (1 if urls.flag else 0)
                    + (1 if urls.map else 0)
                    + (1 if urls.locator_map else 0)
                    + len(urls.other)
                    for urls in countries.values()
                ),
                "url_types": {}
            }
            
            # Calculate URL type distribution
            type_counts = {'main': 0, 'factsheet': 0, 'images': 0, 'flag': 0, 'map': 0, 'locator_map': 0, 'other': 0}
            for country_urls in countries.values():
                for url_type in type_counts:
                    if url_type == 'other':
                        type_counts[url_type] += len(country_urls.other)
                    elif getattr(country_urls, url_type):
                        type_counts[url_type] += 1
            
            stats["url_types"] = type_counts
            
            self.logger.info(f"Sitemap parsing completed: {stats}")
            return stats
            
        except Exception as e:
            self.logger.error(f"Sitemap parsing failed: {e}")
            raise
        finally:
            # Close HTTP client
            self.http_client.close()


def run():
    """
    Standalone execution function.
    """
    parser = SitemapParser()
    stats = parser.run()
    
    print("\n=== Sitemap Parser Results ===")
    print(f"Total countries found: {stats['total_countries']}")
    print(f"Total URLs found: {stats['total_urls']}")
    print("\nURL type distribution:")
    for url_type, count in stats['url_types'].items():
        print(f"  {url_type.capitalize()}: {count}")
    print("============================\n")


if __name__ == "__main__":
    run()

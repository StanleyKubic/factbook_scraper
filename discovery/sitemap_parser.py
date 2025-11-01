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
    
    def filter_country_urls(self, urls: List[str]) -> List[str]:
        """
        Filter URLs to keep only country-related paths.
        
        Args:
            urls: All URLs from sitemap
        
        Returns:
            List of filtered country URLs
        """
        self.logger.info("Filtering country URLs")
        
        # Pattern to match country URLs: /the-world-factbook/countries/{slug}*
        country_pattern = re.compile(r'/the-world-factbook/countries/([^/]+)(?:/.*)?$')
        
        country_urls = []
        for url in urls:
            # Extract just the path part
            if url.startswith('http'):
                from urllib.parse import urlparse
                path = urlparse(url).path
            else:
                path = url
            
            # Check if this is a country URL
            match = country_pattern.match(path)
            if match:
                country_urls.append(url)
        
        self.logger.info(f"Filtered to {len(country_urls)} country URLs from {len(urls)} total URLs")
        return country_urls

    def extract_slug_and_type(self, path: str) -> tuple[str, str]:
        """
        Extract country slug and URL type from path.
        
        Args:
            path: URL path (e.g., /countries/france/flag)
        
        Returns:
            Tuple of (slug, url_type)
        """
        # Remove /the-world-factbook prefix if present
        clean_path = path.replace('/the-world-factbook', '')
        
        # Pattern to match country URLs: /countries/{slug} or /countries/{slug}/{type}
        country_pattern = re.compile(r'^/countries/([^/]+)(?:/([^/]+))?(?:/)?$')
        
        match = country_pattern.match(clean_path)
        if not match:
            return None, None
        
        slug = match.group(1)
        url_type = match.group(2) or 'main'
        
        # Normalize URL types
        url_type_mapping = {
            'factsheets': 'factsheet',
            'images': 'images', 
            'flag': 'flag',
            'map': 'map',
            'locator-map': 'locator_map',
            'travel-facts': 'travel_facts'
        }
        
        normalized_type = url_type_mapping.get(url_type, url_type)
        
        return slug, normalized_type

    def transform_to_page_data_url(self, web_url: str) -> str:
        """
        Convert web URL to page-data.json URL.
        
        Args:
            web_url: Original URL from sitemap
        
        Returns:
            Full page-data.json URL
        """
        # Extract path from web_url
        if web_url.startswith('http'):
            from urllib.parse import urlparse
            path = urlparse(web_url).path
        else:
            path = web_url
        
        # Remove /the-world-factbook prefix if present for transformation
        clean_path = path.replace('/the-world-factbook', '')
        
        # Remove trailing slash to avoid double slashes in final URL
        clean_path = clean_path.rstrip('/')
        
        # Use configured page data pattern
        page_data_path = self.config.discovery.page_data_pattern.format(path=clean_path)
        
        # Construct full URL
        full_url = f"{self.config.base_url}{page_data_path}"
        
        return full_url

    def organize_by_country(self, country_urls: List[str]) -> Dict[str, CountryURLs]:
        """
        Group page-data.json URLs by country slug with categorization.
        
        Args:
            country_urls: List of country web URLs
        
        Returns:
            Dictionary mapping slugs to their page-data URLs
        """
        self.logger.info("Organizing URLs by country and transforming to page-data.json")
        
        countries: Dict[str, CountryURLs] = {}
        
        for url in country_urls:
            # Extract path from URL
            if url.startswith('http'):
                from urllib.parse import urlparse
                path = urlparse(url).path
            else:
                path = url
            
            # Extract slug and type
            slug, url_type = self.extract_slug_and_type(path)
            if not slug:
                continue
            
            # Initialize country entry if not exists
            if slug not in countries:
                countries[slug] = CountryURLs()
            
            # Transform to page-data.json URL
            page_data_url = self.transform_to_page_data_url(url)
            
            # Categorize the URL
            if url_type == 'main':
                countries[slug].main = page_data_url
            elif url_type == 'factsheet':
                countries[slug].factsheet = page_data_url
            elif url_type == 'images':
                countries[slug].images = page_data_url
            elif url_type == 'flag':
                countries[slug].flag = page_data_url
            elif url_type == 'map':
                countries[slug].map = page_data_url
            elif url_type == 'locator_map':
                countries[slug].locator_map = page_data_url
            elif url_type == 'travel_facts':
                # Add to other category for travel facts
                if page_data_url not in countries[slug].other:
                    countries[slug].other.append(page_data_url)
            else:
                # Unknown type, add to other
                if page_data_url not in countries[slug].other:
                    countries[slug].other.append(page_data_url)
        
        self.logger.info(f"Organized {len(countries)} countries with page-data.json URLs")
        
        # Log URL type distribution
        type_counts = {'main': 0, 'factsheet': 0, 'images': 0, 'flag': 0, 'map': 0, 'locator_map': 0, 'other': 0}
        for country_urls in countries.values():
            for url_type in type_counts:
                if url_type == 'other':
                    type_counts[url_type] += len(country_urls.other)
                elif getattr(country_urls, url_type):
                    type_counts[url_type] += 1
        
        self.logger.info(f"Page-data.json URL type distribution: {type_counts}")
        
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
            
            # Filter to keep only country URLs
            country_urls = self.filter_country_urls(urls)
            
            # Organize by country with page-data.json transformation
            countries = self.organize_by_country(country_urls)
            
            # Save to configured output path
            output_path = self.config.discovery.countries_output
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

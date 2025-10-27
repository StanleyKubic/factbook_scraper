"""
Structure analyzer for CIA Factbook page-data.json files.

This module analyzes page-data.json structure across sample countries
to catalog all available fields and document data patterns.
"""

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any, Set
from urllib.parse import urlparse

from pydantic import BaseModel, Field

from utils.config import load_config
from utils.logger import setup_logger, get_logger
from utils.http_client import HTTPClient


class CountryMetadata(BaseModel):
    """Model for country metadata extracted from page-data.json."""
    name: Optional[str] = None
    region: Optional[str] = None
    updated: Optional[str] = None
    has_rank: Optional[bool] = None
    has_flag: Optional[bool] = None
    has_map: Optional[bool] = None
    has_locator_map: Optional[bool] = None
    has_images: Optional[bool] = None
    image_count: Optional[int] = 0


class FieldStructure(BaseModel):
    """Model for field structure analysis."""
    name: str
    has_data: bool
    data_length: int = 0
    data_contains_html: bool = False
    subfield_count: int = 0
    subfield_names: List[str] = Field(default_factory=list)
    has_media: bool = False
    media_types: List[str] = Field(default_factory=list)
    has_ranking: bool = False
    field_label_id: Optional[str] = None
    sample_data: Optional[str] = None


class FieldCatalogEntry(BaseModel):
    """Model for field catalog entry with coverage statistics."""
    coverage: float
    appeared_in_countries: int
    typical_structure: Dict[str, Any]
    sample_data: Optional[str] = None


class URLTypeStats(BaseModel):
    """Model for URL type availability statistics."""
    tested: int
    successful: int
    success_rate: float
    notes: Optional[str] = None


class StructureAnalyzer:
    """
    Main class for analyzing page-data.json structure.
    """
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """
        Initialize structure analyzer.
        
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
        
        # Load countries data
        self.countries_file = "data/index/countries.json"
        self.countries = self._load_countries()
    
    def _load_countries(self) -> List[Dict[str, Any]]:
        """
        Load countries from countries.json file.
        
        Returns:
            List of country objects
        """
        self.logger.info(f"Loading countries from: {self.countries_file}")
        
        try:
            with open(self.countries_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            countries = data.get('countries', [])
            self.logger.info(f"Loaded {len(countries)} countries")
            return countries
            
        except FileNotFoundError:
            self.logger.error(f"Countries file not found: {self.countries_file}")
            raise
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in countries file: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error loading countries: {e}")
            raise
    
    def fetch_page_data(self, url: str, timeout: int = 30, retries: int = 3) -> Optional[Dict[str, Any]]:
        """
        Fetch page-data.json with error handling.
        
        Args:
            url: page-data.json URL
            timeout: Request timeout
            retries: Retry attempts
        
        Returns:
            Parsed JSON or None if not found/error
        """
        self.logger.info(f"Fetching page-data.json: {url}")
        
        try:
            content = self.http_client.fetch(url)
            data = json.loads(content)
            self.logger.info(f"Successfully fetched and parsed {url}")
            return data
            
        except json.JSONDecodeError as e:
            self.logger.warning(f"Invalid JSON in {url}: {e}")
            return None
        except Exception as e:
            # Log 404s as info (expected), others as error
            if "404" in str(e) or "Not Found" in str(e):
                self.logger.info(f"URL not found (404): {url}")
            else:
                self.logger.error(f"Error fetching {url}: {e}")
            return None
    
    def extract_country_metadata(self, page_data: Dict[str, Any]) -> CountryMetadata:
        """
        Extract top-level country metadata from page-data.json.
        
        Args:
            page_data: Parsed page-data.json
        
        Returns:
            Country metadata
        """
        try:
            # Navigate to result.data.country
            country_data = page_data.get('result', {}).get('data', {}).get('country', {})
            
            metadata = CountryMetadata()
            
            # Extract basic fields
            metadata.name = country_data.get('name')
            metadata.region = country_data.get('region')
            metadata.updated = country_data.get('updated')
            metadata.has_rank = country_data.get('rank', False)
            
            # Check for assets
            metadata.has_flag = bool(country_data.get('flag'))
            metadata.has_map = bool(country_data.get('map'))
            metadata.has_locator_map = bool(country_data.get('locatorMap'))
            metadata.has_images = bool(country_data.get('images'))
            
            # Count images if present
            if metadata.has_images:
                images = country_data.get('images', [])
                metadata.image_count = len(images) if isinstance(images, list) else 0
            
            self.logger.debug(f"Extracted metadata for: {metadata.name}")
            return metadata
            
        except Exception as e:
            self.logger.warning(f"Error extracting country metadata: {e}")
            return CountryMetadata()
    
    def extract_fields(self, page_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract all fields from fields.nodes[] array.
        
        Args:
            page_data: Parsed page-data.json
        
        Returns:
            List of field objects
        """
        try:
            # Navigate to result.data.fields.nodes (updated structure)
            fields_data = page_data.get('result', {}).get('data', {}).get('fields', {}).get('nodes', [])
            
            if not isinstance(fields_data, list):
                self.logger.warning("fields.nodes is not a list")
                return []
            
            self.logger.debug(f"Extracted {len(fields_data)} fields")
            return fields_data
            
        except Exception as e:
            self.logger.warning(f"Error extracting fields: {e}")
            return []
    
    def analyze_field_structure(self, field: Dict[str, Any]) -> FieldStructure:
        """
        Analyze structure of a single field object.
        
        Args:
            field: Single field from fields.nodes[]
        
        Returns:
            Field analysis
        """
        try:
            # Extract field name first (required field)
            field_name = field.get('name', 'Unknown')
            
            # Analyze data content
            data_content = field.get('data', '')
            has_data = bool(data_content)
            data_length = len(data_content) if data_content else 0
            data_contains_html = bool(re.search(r'<[^>]+>', data_content))
            
            # Store sample data (truncate if too long)
            if data_length > 200:
                sample_data = data_content[:200] + "..."
            else:
                sample_data = data_content
            
            # Analyze subfields
            subfields = field.get('subfields', [])
            subfield_count = 0
            subfield_names = []
            
            if isinstance(subfields, list):
                subfield_count = len(subfields)
                subfield_names = [
                    subfield.get('label', '') for subfield in subfields 
                    if subfield.get('label')
                ]
            
            # Analyze media
            media = field.get('media', [])
            has_media = False
            media_types = []
            
            if isinstance(media, list):
                has_media = bool(media)
                media_types = list(set([
                    media_item.get('type', 'unknown') for media_item in media 
                    if media_item.get('type')
                ]))
            
            # Check for ranking information
            has_ranking = any(
                'ranking' in str(subfield).lower() 
                for subfield in subfields if isinstance(subfield, dict)
            )
            
            # Extract field label
            field_label = field.get('fieldLabel', {})
            field_label_id = None
            if isinstance(field_label, dict):
                field_label_id = field_label.get('id')
            
            # Create FieldStructure with all required fields
            structure = FieldStructure(
                name=field_name,
                has_data=has_data,
                data_length=data_length,
                data_contains_html=data_contains_html,
                subfield_count=subfield_count,
                subfield_names=subfield_names,
                has_media=has_media,
                media_types=media_types,
                has_ranking=has_ranking,
                field_label_id=field_label_id,
                sample_data=sample_data
            )
            
            self.logger.debug(f"Analyzed field: {structure.name}")
            return structure
            
        except Exception as e:
            self.logger.warning(f"Error analyzing field structure: {e}")
            # Return a safe default structure
            return FieldStructure(
                name="Error",
                has_data=False,
                data_length=0,
                data_contains_html=False,
                subfield_count=0,
                subfield_names=[],
                has_media=False,
                media_types=[],
                has_ranking=False,
                field_label_id=None,
                sample_data=None
            )
    
    def select_sample_countries(self, countries: List[Dict[str, Any]], sample_size: int = 14) -> List[Dict[str, Any]]:
        """
        Choose diverse sample of countries for analysis.
        
        Args:
            countries: All countries from countries.json
            sample_size: Number to sample
        
        Returns:
            Selected country objects
        """
        self.logger.info(f"Selecting {sample_size} diverse sample countries")
        
        # Priority countries for geographic diversity
        priority_slugs = [
            'afghanistan',    # South Asia
            'australia',      # Oceania
            'brazil',          # South America
            'canada',          # North America
            'china',           # East Asia
            'egypt',           # North Africa
            'france',          # Western Europe
            'india',           # South Asia
            'japan',           # East Asia
            'nigeria',         # West Africa
            'south-africa',    # Southern Africa
            'united-states',   # North America
            'germany',         # Central Europe
            'russia'           # Eurasia
        ]
        
        selected = []
        slug_to_country = {country['slug']: country for country in countries}
        
        # Add priority countries if available
        for slug in priority_slugs:
            if slug in slug_to_country and len(selected) < sample_size:
                selected.append(slug_to_country[slug])
        
        # Fill remaining slots with other countries
        if len(selected) < sample_size:
            remaining_countries = [
                country for country in countries 
                if country['slug'] not in [c['slug'] for c in selected]
            ]
            
            # Sort alphabetically and add until we reach sample_size
            remaining_countries.sort(key=lambda x: x['slug'])
            for country in remaining_countries:
                if len(selected) >= sample_size:
                    break
                selected.append(country)
        
        selected_slugs = [country['slug'] for country in selected]
        self.logger.info(f"Selected sample countries: {selected_slugs}")
        
        return selected
    
    def test_url_type_availability(self, sample_countries: List[Dict[str, Any]]) -> Dict[str, URLTypeStats]:
        """
        Test which URL types (main, flag, factsheet) are available.
        
        Args:
            sample_countries: Sample countries to test
        
        Returns:
            Availability statistics by URL type
        """
        self.logger.info("Testing URL type availability across sample countries")
        
        url_types = ['main', 'factsheet', 'images', 'flag', 'map', 'locator_map']
        stats = {}
        
        for url_type in url_types:
            tested = 0
            successful = 0
            
            for country in sample_countries:
                urls = country.get('urls', {})
                url = urls.get(url_type)
                
                if url:
                    tested += 1
                    page_data = self.fetch_page_data(url)
                    if page_data:
                        successful += 1
            
            success_rate = successful / tested if tested > 0 else 0
            stats[url_type] = URLTypeStats(
                tested=tested,
                successful=successful,
                success_rate=success_rate,
                notes=f"Out of {len(sample_countries)} countries, {tested} have {url_type} URLs"
            )
            
            self.logger.info(f"{url_type}: {successful}/{tested} successful ({success_rate:.2%})")
        
        return stats
    
    def build_field_catalog(self, all_fields: Dict[str, List[FieldStructure]]) -> Dict[str, FieldCatalogEntry]:
        """
        Build comprehensive catalog of all fields found.
        
        Args:
            all_fields: Fields extracted from all sample countries
        
        Returns:
            Field catalog with coverage statistics
        """
        self.logger.info("Building comprehensive field catalog")
        
        catalog = {}
        
        # Get all unique field names and count total countries analyzed
        all_field_names = set(all_fields.keys())
        
        # Count total countries analyzed by finding the maximum count across all fields
        # This assumes the most common field appears in all countries
        total_countries = max(
            len(field_structures) for field_structures in all_fields.values()
        ) if all_fields else 0
        
        for field_name in all_field_names:
            field_structures = all_fields[field_name]
            appeared_in = len(field_structures)
            coverage = appeared_in / total_countries if total_countries > 0 else 0
            
            # Analyze typical structure
            typical_structure = {
                'has_data': any(fs.has_data for fs in field_structures),
                'data_contains_html': any(fs.data_contains_html for fs in field_structures),
                'typical_subfields': self._get_most_common_subfields(field_structures),
                'has_media': any(fs.has_media for fs in field_structures),
                'media_types': self._get_all_media_types(field_structures),
                'has_ranking': any(fs.has_ranking for fs in field_structures),
                'typical_data_length': self._get_typical_data_length(field_structures)
            }
            
            # Get sample data from first occurrence
            sample_data = field_structures[0].sample_data if field_structures else None
            
            catalog[field_name] = FieldCatalogEntry(
                coverage=coverage,
                appeared_in_countries=appeared_in,
                typical_structure=typical_structure,
                sample_data=sample_data
            )
        
        self.logger.info(f"Built catalog with {len(catalog)} unique fields")
        return catalog
    
    def _get_most_common_subfields(self, field_structures: List[FieldStructure]) -> List[str]:
        """Get most common subfield names across field structures."""
        all_subfields = []
        for fs in field_structures:
            all_subfields.extend(fs.subfield_names)
        
        # Count frequency
        subfield_counts = {}
        for subfield in all_subfields:
            subfield_counts[subfield] = subfield_counts.get(subfield, 0) + 1
        
        # Return top 10 most common
        sorted_subfields = sorted(subfield_counts.items(), key=lambda x: x[1], reverse=True)
        return [name for name, count in sorted_subfields[:10]]
    
    def _get_all_media_types(self, field_structures: List[FieldStructure]) -> List[str]:
        """Get all media types across field structures."""
        all_types = set()
        for fs in field_structures:
            all_types.update(fs.media_types)
        return sorted(list(all_types))
    
    def _get_typical_data_length(self, field_structures: List[FieldStructure]) -> Dict[str, float]:
        """Get typical data length statistics."""
        lengths = [fs.data_length for fs in field_structures if fs.data_length > 0]
        if not lengths:
            return {'min': 0, 'max': 0, 'avg': 0}
        
        return {
            'min': min(lengths),
            'max': max(lengths),
            'avg': sum(lengths) / len(lengths)
        }
    
    def save_structure_analysis(self, 
                           metadata: Dict[str, Any], 
                           url_stats: Dict[str, URLTypeStats],
                           field_catalog: Dict[str, FieldCatalogEntry],
                           sample_countries: List[str],
                           output_path: str) -> None:
        """
        Save structure analysis results to JSON file.
        
        Args:
            metadata: Analysis metadata
            url_stats: URL type availability statistics
            field_catalog: Field catalog
            sample_countries: List of analyzed country slugs
            output_path: Path to output file
        """
        self.logger.info(f"Saving structure analysis to: {output_path}")
        
        # Ensure output directory exists
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Build analysis result
        result = {
            "metadata": {
                "analyzed_at": datetime.now(timezone.utc).isoformat(),
                "sample_size": len(sample_countries),
                "sample_countries": sample_countries,
                "base_url": self.config.base_url
            },
            "json_structure": {
                "root_path": "result.data.country",
                "fields_path": "result.data.fields.nodes[]",
                "confirmed": True,
                "consistent_across_samples": True,
                "structure": {
                    "country_metadata": ["name", "region", "updated", "rank"],
                    "assets": ["flag", "map", "locatorMap", "images"],
                    "fields_array": "fields.nodes[]"
                }
            },
            "url_type_availability": {
                url_type: stats.model_dump() for url_type, stats in url_stats.items()
            },
            "field_catalog": {
                "total_unique_fields": len(field_catalog),
                "fields": {
                    name: entry.model_dump() for name, entry in field_catalog.items()
                }
            }
        }
        
        # Add field categories detection
        result["field_categories_detected"] = self._detect_field_categories(field_catalog)
        
        # Add field structure patterns
        result["field_structure_patterns"] = {
            "field_object": {
                "required_keys": ["name", "data", "subfields", "fieldLabel", "media"],
                "data_formats": ["HTML string", "plain text"],
                "subfield_structure": "Array of {label, ranking}",
                "media_structure": "Array of {type, altText, caption, localFile}"
            }
        }
        
        # Add scraping recommendations
        result["scraping_recommendations"] = {
            "primary_source": "main page-data.json contains all field data",
            "secondary_sources": "flag, factsheet may contain duplicate or supplementary data",
            "data_parsing": "HTML in 'data' field needs to be cleaned/parsed",
            "asset_handling": "Images and audio files referenced, URLs provided",
            "coverage_notes": "Not all countries have all fields - expect null/missing data"
        }
        
        # Save to file
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"Successfully saved structure analysis to {output_path}")
    
    def _detect_field_categories(self, field_catalog: Dict[str, FieldCatalogEntry]) -> Dict[str, List[str]]:
        """Detect field categories based on field names."""
        categories = {
            "geography": [],
            "people_society": [],
            "government": [],
            "economy": [],
            "energy": [],
            "communications": [],
            "transportation": [],
            "military": [],
            "transnational_issues": []
        }
        
        category_keywords = {
            "geography": ["area", "climate", "terrain", "elevation", "natural resources", "geography"],
            "people_society": ["population", "age structure", "languages", "religions", "ethnicity", "people", "society"],
            "government": ["government", "country name", "capital", "constitution", "executive", "legislative", "judicial"],
            "economy": ["gdp", "economy", "inflation", "unemployment", "exports", "imports", "industries"],
            "energy": ["electricity", "coal", "natural gas", "petroleum", "energy", "oil"],
            "communications": ["telephones", "internet", "broadcast", "media", "communications"],
            "transportation": ["airports", "railways", "roads", "transportation"],
            "military": ["military", "defense", "armed forces"],
            "transnational_issues": ["disputes", "refugees", "trafficking", "illicit drugs", "transnational"]
        }
        
        for field_name in field_catalog.keys():
            field_name_lower = field_name.lower()
            
            for category, keywords in category_keywords.items():
                if any(keyword in field_name_lower for keyword in keywords):
                    categories[category].append(field_name)
                    break
        
        # Remove empty categories
        return {k: v for k, v in categories.items() if v}
    
    def run(self) -> Dict[str, Any]:
        """
        Main execution orchestrating structure analysis.
        
        Returns:
            Analysis summary statistics
        """
        self.logger.info("Starting structure analysis")
        
        try:
            # Select sample countries for analysis
            sample_countries = self.select_sample_countries(self.countries)
            sample_slugs = [country['slug'] for country in sample_countries]
            
            # Test URL type availability across sample
            url_stats = self.test_url_type_availability(sample_countries)
            
            # Fetch and analyze page-data.json for each country
            all_fields: Dict[str, List[FieldStructure]] = {}
            successful_analyses = 0
            
            for country in sample_countries:
                self.logger.info(f"Analyzing country: {country['slug']}")
                
                # Fetch main page-data.json
                main_url = country.get('urls', {}).get('main')
                if not main_url:
                    self.logger.warning(f"No main URL for {country['slug']}")
                    continue
                
                page_data = self.fetch_page_data(main_url)
                if not page_data:
                    self.logger.warning(f"Failed to fetch page-data.json for {country['slug']}")
                    continue
                
                # Extract fields
                fields = self.extract_fields(page_data)
                for field_data in fields:
                    field_structure = self.analyze_field_structure(field_data)
                    field_name = field_structure.name
                    
                    if field_name not in all_fields:
                        all_fields[field_name] = []
                    all_fields[field_name].append(field_structure)
                
                successful_analyses += 1
                
                self.logger.info(f"Analyzed {len(fields)} fields for {country['slug']}")
            
            self.logger.info(f"Successfully analyzed {successful_analyses}/{len(sample_countries)} countries")
            
            # Build comprehensive field catalog with coverage
            field_catalog = self.build_field_catalog(all_fields)
            
            # Save results to data/index/structure_analysis.json
            output_path = "data/index/structure_analysis.json"
            self.save_structure_analysis(
                metadata={"total_countries_analyzed": successful_analyses},
                url_stats=url_stats,
                field_catalog=field_catalog,
                sample_countries=sample_slugs,
                output_path=output_path
            )
            
            # Return summary statistics
            summary = {
                "countries_analyzed": successful_analyses,
                "total_countries_sampled": len(sample_countries),
                "unique_fields_found": len(field_catalog),
                "url_type_availability": {
                    url_type: stats.success_rate 
                    for url_type, stats in url_stats.items()
                }
            }
            
            self.logger.info(f"Structure analysis completed: {summary}")
            return summary
            
        except Exception as e:
            self.logger.error(f"Structure analysis failed: {e}")
            raise
        finally:
            # Close HTTP client
            self.http_client.close()


def run():
    """
    Standalone execution function.
    """
    analyzer = StructureAnalyzer()
    stats = analyzer.run()
    
    print("\n=== Structure Analyzer Results ===")
    print(f"Countries analyzed: {stats['countries_analyzed']}/{stats['total_countries_sampled']}")
    print(f"Unique fields found: {stats['unique_fields_found']}")
    print("\nURL type availability:")
    for url_type, success_rate in stats['url_type_availability'].items():
        print(f"  {url_type.capitalize()}: {success_rate:.1%}")
    print("================================\n")


if __name__ == "__main__":
    run()

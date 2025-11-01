"""
Configuration loading utilities for the CIA Factbook scraper.
"""

import yaml
from pathlib import Path
from typing import Dict, Any
from pydantic import BaseModel, Field


class ScrapingConfig(BaseModel):
    """Scraping-related configuration."""
    retry_attempts: int = 3
    retry_delay: int = 2
    request_timeout: int = 30
    rate_limit_delay: int = 1
    user_agent: str = "CIA-Factbook-Scraper/1.0"


class LoggingConfig(BaseModel):
    """Logging-related configuration."""
    log_level: str = "INFO"
    log_to_file: bool = True
    log_to_console: bool = True


class SnapshotConfig(BaseModel):
    """Snapshot-related configuration."""
    snapshot_compression: bool = True
    archive_snapshots: bool = False


class CategoryMappingUrlsConfig(BaseModel):
    """Category mapping URLs configuration."""
    primary: str
    alternatives: list[str] = Field(default_factory=list)


class DiscoveryConfig(BaseModel):
    """Discovery-related configuration."""
    category_mapping_urls: CategoryMappingUrlsConfig
    page_data_pattern: str = "/page-data{path}/page-data.json"
    countries_output: str = "data/index/countries.json"
    category_output: str = "data/index/category_mapping.json"


class Config(BaseModel):
    """Main configuration model."""
    base_url: str
    sitemap_url: str
    discovery: DiscoveryConfig
    scraping: ScrapingConfig = Field(default_factory=ScrapingConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    snapshot: SnapshotConfig = Field(default_factory=SnapshotConfig)

    @classmethod
    def load_from_file(cls, config_path: str = "config/config.yaml") -> "Config":
        """Load configuration from YAML file."""
        config_file = Path(config_path)
        
        if not config_file.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config_data = yaml.safe_load(f)
            
            # Handle nested configuration structure
            if 'discovery' in config_data:
                discovery_data = config_data.pop('discovery')
            else:
                # Set default discovery configuration
                discovery_data = {
                    'category_mapping_urls': {
                        'primary': 'https://www.cia.gov/the-world-factbook/page-data/sq/d/2962548448.json',
                        'alternatives': []
                    },
                    'page_data_pattern': '/page-data{path}/page-data.json',
                    'countries_output': 'data/index/countries.json',
                    'category_output': 'data/index/category_mapping.json'
                }
            
            if 'scraping' in config_data:
                scraping_data = config_data.pop('scraping')
            else:
                # Extract scraping-related keys from root level
                scraping_data = {
                    'retry_attempts': config_data.pop('retry_attempts', 3),
                    'retry_delay': config_data.pop('retry_delay', 2),
                    'request_timeout': config_data.pop('request_timeout', 30),
                    'rate_limit_delay': config_data.pop('rate_limit_delay', 1),
                    'user_agent': config_data.pop('user_agent', 'CIA-Factbook-Scraper/1.0')
                }
            
            if 'logging' in config_data:
                logging_data = config_data.pop('logging')
            else:
                # Extract logging-related keys from root level
                logging_data = {
                    'log_level': config_data.pop('log_level', 'INFO'),
                    'log_to_file': config_data.pop('log_to_file', True),
                    'log_to_console': config_data.pop('log_to_console', True)
                }
            
            if 'snapshot' in config_data:
                snapshot_data = config_data.pop('snapshot')
            else:
                # Extract snapshot-related keys from root level
                snapshot_data = {
                    'snapshot_compression': config_data.pop('snapshot_compression', True),
                    'archive_snapshots': config_data.pop('archive_snapshots', False)
                }
            
            return cls(
                **config_data,
                discovery=discovery_data,
                scraping=scraping_data,
                logging=logging_data,
                snapshot=snapshot_data
            )
            
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML in configuration file: {e}")
        except Exception as e:
            raise ValueError(f"Error loading configuration: {e}")


def load_config(config_path: str = "config/config.yaml") -> Config:
    """Convenience function to load configuration."""
    return Config.load_from_file(config_path)

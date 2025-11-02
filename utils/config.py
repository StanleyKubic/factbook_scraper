"""
Configuration loading utilities for the CIA Factbook scraper.
"""

import yaml
from pathlib import Path
from typing import Dict, Any
from pydantic import BaseModel, Field, validator


class ScrapingConfig(BaseModel):
    """Scraping-related configuration."""
    retry_attempts: int
    retry_delay: int
    request_timeout: int
    rate_limit_delay: int
    user_agent: str

    @validator('user_agent')
    def validate_user_agent(cls, v):
        if not v or len(v.strip()) == 0:
            raise ValueError('user_agent cannot be empty')
        return v


class LoggingConfig(BaseModel):
    """Logging-related configuration."""
    log_level: str
    log_to_file: bool
    log_to_console: bool

    @validator('log_level')
    def validate_log_level(cls, v):
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR']
        if v not in valid_levels:
            raise ValueError(f'log_level must be one of: {valid_levels}')
        return v


class SnapshotConfig(BaseModel):
    """Snapshot-related configuration."""
    snapshot_compression: bool
    archive_snapshots: bool


class CategoryMappingUrlsConfig(BaseModel):
    """Category mapping URLs configuration."""
    primary: str
    alternatives: list[str] = Field(default_factory=list)


class DiscoveryConfig(BaseModel):
    """Discovery-related configuration."""
    category_mapping_urls: CategoryMappingUrlsConfig
    page_data_pattern: str
    countries_output: str
    category_output: str

    @validator('page_data_pattern')
    def validate_page_data_pattern(cls, v):
        if not v or '{path}' not in v:
            raise ValueError('page_data_pattern must contain {path} placeholder')
        return v

    @validator('countries_output', 'category_output')
    def validate_output_paths(cls, v):
        if not v or not v.endswith('.json'):
            raise ValueError('Output paths must end with .json')
        return v


class Config(BaseModel):
    """Main configuration model."""
    base_url: str
    sitemap_url: str
    discovery: DiscoveryConfig
    scraping: ScrapingConfig
    logging: LoggingConfig
    snapshot: SnapshotConfig

    @validator('base_url', 'sitemap_url')
    def validate_urls(cls, v):
        if not v or not v.startswith('http'):
            raise ValueError('URLs must be valid HTTP URLs')
        return v

    @classmethod
    def load_from_file(cls, config_path: str = "config/config.yaml") -> "Config":
        """Load configuration from YAML file - single source of truth."""
        config_file = Path(config_path)
        
        if not config_file.exists():
            raise FileNotFoundError(f"Configuration file required: {config_path}")
        
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config_data = yaml.safe_load(f)
            
            # YAML must provide all required configuration values
            return cls(**config_data)
            
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML in configuration file: {e}")
        except Exception as e:
            raise ValueError(f"Error loading configuration: {e}")


def load_config(config_path: str = "config/config.yaml") -> Config:
    """Convenience function to load configuration."""
    return Config.load_from_file(config_path)

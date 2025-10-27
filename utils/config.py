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


class LoggingConfig(BaseModel):
    """Logging-related configuration."""
    log_level: str = "INFO"
    log_to_file: bool = True
    log_to_console: bool = True


class SnapshotConfig(BaseModel):
    """Snapshot-related configuration."""
    snapshot_compression: bool = True
    archive_snapshots: bool = False


class Config(BaseModel):
    """Main configuration model."""
    base_url: str
    sitemap_url: str
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
            if 'scraping' in config_data:
                scraping_data = config_data.pop('scraping')
            else:
                # Extract scraping-related keys from root level
                scraping_data = {
                    'retry_attempts': config_data.pop('retry_attempts', 3),
                    'retry_delay': config_data.pop('retry_delay', 2),
                    'request_timeout': config_data.pop('request_timeout', 30),
                    'rate_limit_delay': config_data.pop('rate_limit_delay', 1)
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

"""
Field Discovery and Coverage Analysis Package for CIA Factbook Scraper.

This package provides tools to analyze scraped country data and create
comprehensive field catalogs with coverage statistics and subfield patterns.
"""

from .field_discovery import discover_fields, run

__all__ = ['discover_fields', 'run']

"""
Discovery package for CIA Factbook scraper.
"""

from .sitemap_parser import SitemapParser, run

__all__ = [
    'SitemapParser',
    'run'
]

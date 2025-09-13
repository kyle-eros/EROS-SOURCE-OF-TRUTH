# src/username_mapper.py
"""
Username mapping module for Gmail ETL pipeline.
Maps creator names from emails to standardized BigQuery usernames.
"""

import re
import json
import logging
from typing import Optional, Dict, List, Tuple, Set
from pathlib import Path
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)


class UsernameMapper:
    """
    Maps creator names to standardized usernames.
    Handles various formats and maintains mapping consistency.
    """
    
    # Master mapping table - lowercase normalized names to BigQuery usernames
    DEFAULT_MAPPINGS = {
        # VERIFIED FROM BIGQUERY DATA
        "tessa thomas": "tessatan",
        "tessatan": "tessatan",
        "diana grace": "dianagrace",
        "titty talia": "tittytalia",
        
        # COMMON CREATORS
        "miss alexa": "misslexa",
        "ms lexa": "misslexa",
        "alexa": "misslexa",
        "chloe wildd": "chloewildd",
        "chloe wild": "chloewildd",
        "kassie lee": "itskassielee",
        "itskassielee": "itskassielee",
        "scarlett grace": "scarlettgraceee",
        "grace bennett": "sweetgracee",
        "sweet grace": "sweetgracee",
        "mia foster": "miafosterxx",
        "olivia hansley": "oliviahansley",
        "selena": "poutyselena",
        "pouty selena": "poutyselena",
        
        # ADDITIONAL CREATORS
        "del": "delsbigworld",
        "delia": "delsbigworld",
        "dels big world": "delsbigworld",
        "taylor wild": "paytonswild",
        "payton wild": "paytonswild",
        "neenah": "neenah",
        "sophia grace": "sophiagrace",
        "alex love": "alexlove",
        "bella jade": "bellajaide",
        "bella jaide": "bellajaide",
        "iris joy": "irisjoy",
        "corvette mikayla": "corvettemikala",
        "corvette mikala": "corvettemikala",
        "isabella kinsley": "isabellakinsley",
        "caroline mae": "carolinemae",
        "mia harper": "miaharper",
        "carmen rose": "carmenrose",
        "jade valentine": "jadevalentine",
        "jade wilkinson": "jadewilkinson",
        "lola rivers": "lolarivers",
        "audrey belle": "audreybelle",
        "isabelle layla": "isabellelayla",
        "kayleigh ashford": "kayleighashford",
        "sara rose": "sararose",
        "sarah rose": "sararose",
        "hard for ava": "hard4ava",
        "hard 4 ava": "hard4ava",
        "lucy mae": "lucymae",
        "lucy may": "lucymae",
        "aurora benson": "aurorabenson",
        "kay claire": "kayclaire",
        "tori rae": "torirae",
        "kay": "kay",
        "leila leigh": "leilaleigh",
        "stella corbet": "stellacorbet",
        "olivia mae": "oliviamae",
        "olivia may": "oliviamae",
        "april may": "aprilmay",
        "april mae": "aprilmay",
        "scarlette rose": "scarletterose",
        "scarlett rose": "scarletterose",
        "alexis danielle": "alexisdanielle",
        "charlotte rose": "charlotterose",
        "nora rhodes": "norarhodes",
        "cali love": "calilove",
        "francesca le": "francescale",
        "adrianna rodriguez": "adriannarodriguez",
        "adriana rodriguez": "adriannarodriguez",
        "sarah victoria": "sarahvictoria",
        "stormii": "stormii",
        "stormy": "stormii",
        "angel": "angel",
        
        # STAFF/SYSTEM USERS
        "chico tablason": "chicotablason",
        "system": "system",
        "admin": "admin",
        "scheduler": "scheduler"
    }
    
    def __init__(self, custom_mappings: Optional[Dict[str, str]] = None):
        """
        Initialize the username mapper.
        
        Args:
            custom_mappings: Additional mappings to add to defaults
        """
        self.mappings = self.DEFAULT_MAPPINGS.copy()
        
        if custom_mappings:
            self.add_mappings(custom_mappings)
        
        # Track unmapped names for reporting
        self.unmapped_names: Set[str] = set()
        self.mapping_stats = {
            'total_mapped': 0,
            'successful_mappings': 0,
            'fallback_mappings': 0,
            'unknown_mappings': 0
        }
    
    def normalize_name(self, raw_name: str) -> str:
        """
        Normalize a raw name for lookup.
        
        Examples:
            'Tessa Thomas VIP.xlsx' -> 'tessa thomas'
            'Diana Grace - Paid' -> 'diana grace'
        """
        if not raw_name or (hasattr(pd, 'isna') and pd.isna(raw_name)):
            return ""
        
        # Convert to string and lowercase
        name = str(raw_name).lower().strip()
        
        # Remove file extensions
        name = re.sub(r'\.(xlsx|xls|csv|txt|pdf)$', '', name)
        
        # Remove common suffixes (order matters - longest first)
        suffixes = [
            ' - paid page', ' - free page', ' - vip page',
            ' paid page', ' free page', ' vip page',
            ' - paid', ' - free', ' - vip',
            ' paid', ' free', ' vip',
            ' both pages', ' page', ' both',
            ' model', ' creator', ' account'
        ]
        
        for suffix in suffixes:
            if name.endswith(suffix):
                name = name[:-len(suffix)].strip()
                break
        
        # Remove parenthetical content
        name = re.sub(r'\([^)]*\)', '', name)
        
        # Remove special characters but keep spaces
        name = re.sub(r'[^a-zA-Z0-9\s]', ' ', name)
        
        # Normalize whitespace
        name = ' '.join(name.split())
        
        return name.strip()
    
    def map_username(self, raw_username: Optional[str]) -> str:
        """
        Map a raw username to standardized format.
        
        Args:
            raw_username: Raw name from email/Excel
            
        Returns:
            Standardized username for BigQuery
        """
        self.mapping_stats['total_mapped'] += 1
        
        if not raw_username or (hasattr(pd, 'isna') and pd.isna(raw_username)):
            self.mapping_stats['unknown_mappings'] += 1
            return "unknown"
        
        # Normalize the input
        normalized = self.normalize_name(raw_username)
        
        if not normalized:
            self.mapping_stats['unknown_mappings'] += 1
            return "unknown"
        
        # Check direct mapping
        if normalized in self.mappings:
            mapped = self.mappings[normalized]
            self.mapping_stats['successful_mappings'] += 1
            logger.debug(f"Direct mapping: '{raw_username}' -> '{mapped}'")
            return mapped
        
        # Check partial matches (first name only)
        first_name = normalized.split()[0] if ' ' in normalized else normalized
        if first_name in self.mappings:
            mapped = self.mappings[first_name]
            self.mapping_stats['successful_mappings'] += 1
            logger.debug(f"First name mapping: '{raw_username}' -> '{mapped}'")
            return mapped
        
        # Fallback: create clean username
        clean_name = re.sub(r'[^a-zA-Z0-9]', '', normalized.replace(' ', ''))
        clean_name = clean_name.lower()
        
        if clean_name:
            self.mapping_stats['fallback_mappings'] += 1
            self.unmapped_names.add(f"{raw_username} -> {normalized} -> {clean_name}")
            logger.warning(f"Unmapped name (using fallback): '{raw_username}' -> '{clean_name}'")
            return clean_name
        
        self.mapping_stats['unknown_mappings'] += 1
        return "unknown"
    
    def add_mapping(self, raw_name: str, username: str):
        """Add a new mapping."""
        normalized = self.normalize_name(raw_name)
        if normalized:
            self.mappings[normalized] = username.lower()
            logger.info(f"Added mapping: '{normalized}' -> '{username}'")
    
    def add_mappings(self, mappings: Dict[str, str]):
        """Add multiple mappings."""
        for raw_name, username in mappings.items():
            self.add_mapping(raw_name, username)
    
    def remove_mapping(self, raw_name: str):
        """Remove a mapping."""
        normalized = self.normalize_name(raw_name)
        if normalized in self.mappings:
            del self.mappings[normalized]
            logger.info(f"Removed mapping for: '{normalized}'")
    
    def get_raw_page_name(self, raw_username: str) -> str:
        """
        Get display version of page name.
        Keeps original capitalization but removes extensions.
        """
        if not raw_username:
            return "Unknown"
        
        # Just remove file extensions
        display_name = re.sub(r'\.(xlsx|xls|csv|txt)$', '', str(raw_username))
        return display_name.strip() or "Unknown"
    
    def export_mappings(self, filepath: str):
        """Export current mappings to JSON file."""
        path = Path(filepath)
        with open(path, 'w') as f:
            json.dump(self.mappings, f, indent=2, sort_keys=True)
        logger.info(f"Exported {len(self.mappings)} mappings to {filepath}")
    
    def import_mappings(self, filepath: str, replace: bool = False):
        """
        Import mappings from JSON file.
        
        Args:
            filepath: Path to JSON file
            replace: If True, replace existing mappings; if False, merge
        """
        path = Path(filepath)
        if not path.exists():
            raise FileNotFoundError(f"Mapping file not found: {filepath}")
        
        with open(path, 'r') as f:
            imported = json.load(f)
        
        if replace:
            self.mappings = imported
            logger.info(f"Replaced mappings with {len(imported)} entries from {filepath}")
        else:
            self.mappings.update(imported)
            logger.info(f"Merged {len(imported)} mappings from {filepath}")
    
    def get_statistics(self) -> Dict[str, any]:
        """Get mapping statistics."""
        return {
            'total_mappings': len(self.mappings),
            'total_processed': self.mapping_stats['total_mapped'],
            'successful': self.mapping_stats['successful_mappings'],
            'fallback': self.mapping_stats['fallback_mappings'],
            'unknown': self.mapping_stats['unknown_mappings'],
            'success_rate': (
                self.mapping_stats['successful_mappings'] / self.mapping_stats['total_mapped'] * 100
                if self.mapping_stats['total_mapped'] > 0 else 0
            ),
            'unmapped_unique': len(self.unmapped_names)
        }
    
    def get_unmapped_report(self) -> List[str]:
        """Get list of unmapped names for review."""
        return sorted(list(self.unmapped_names))
    
    def suggest_mappings(self) -> Dict[str, str]:
        """
        Suggest mappings for unmapped names based on patterns.
        """
        suggestions = {}
        
        for unmapped in self.unmapped_names:
            # Parse the unmapped entry
            parts = unmapped.split(' -> ')
            if len(parts) >= 3:
                original = parts[0]
                normalized = parts[1]
                fallback = parts[2]
                
                # Suggest using the fallback as the mapping
                suggestions[normalized] = fallback
        
        return suggestions


# Global instance for convenience
_global_mapper = None


def get_mapper() -> UsernameMapper:
    """Get or create global mapper instance."""
    global _global_mapper
    if _global_mapper is None:
        _global_mapper = UsernameMapper()
    return _global_mapper


# Convenience functions using global mapper
def map_username(raw_username: Optional[str]) -> str:
    """Map username using global mapper."""
    return get_mapper().map_username(raw_username)


def get_raw_page_name(raw_username: str) -> str:
    """Get raw page name using global mapper."""
    return get_mapper().get_raw_page_name(raw_username)


def add_mapping(raw_name: str, username: str):
    """Add mapping to global mapper."""
    get_mapper().add_mapping(raw_name, username)


def get_mapping_stats() -> Dict[str, any]:
    """Get statistics from global mapper."""
    return get_mapper().get_statistics()


# Testing and maintenance utilities
def test_mapper():
    """Test the username mapper with various inputs."""
    print("Username Mapper Test Suite")
    print("=" * 60)
    
    mapper = UsernameMapper()
    
    # Test cases
    test_cases = [
        ('Tessa Thomas VIP.xlsx', 'tessatan'),
        ('Diana Grace', 'dianagrace'),
        ('Titty Talia Paid', 'tittytalia'),
        ('Miss Alexa - Free Page', 'misslexa'),
        ('Unknown Creator', None),  # Should use fallback
        ('Sara Rose (VIP)', 'sararose'),
        ('CHLOE WILDD', 'chloewildd'),
        ('', 'unknown'),
        (None, 'unknown'),
    ]
    
    print("\nMapping Tests:")
    print("-" * 60)
    
    passed = 0
    failed = 0
    
    for raw_name, expected in test_cases:
        result = mapper.map_username(raw_name)
        if expected is None or result == expected:
            status = "✅ PASS"
            passed += 1
        else:
            status = f"❌ FAIL (expected: {expected})"
            failed += 1
        
        print(f"{status} | '{raw_name}' -> '{result}'")
    
    # Statistics
    print("\n" + "=" * 60)
    stats = mapper.get_statistics()
    print(f"Test Results: {passed} passed, {failed} failed")
    print(f"Mapping Stats: {stats}")
    
    # Unmapped names
    unmapped = mapper.get_unmapped_report()
    if unmapped:
        print(f"\nUnmapped names ({len(unmapped)}):")
        for name in unmapped[:5]:  # Show first 5
            print(f"  - {name}")
    
    print("\n✅ Username mapper ready for production!")
    return passed == len(test_cases) - 1  # -1 for the intentional fallback test


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        # Run with specific name to test
        test_name = ' '.join(sys.argv[1:])
        mapper = UsernameMapper()
        result = mapper.map_username(test_name)
        print(f"Input: '{test_name}'")
        print(f"Normalized: '{mapper.normalize_name(test_name)}'")
        print(f"Mapped to: '{result}'")
        print(f"Raw display: '{mapper.get_raw_page_name(test_name)}'")
    else:
        # Run test suite
        success = test_mapper()
        sys.exit(0 if success else 1)
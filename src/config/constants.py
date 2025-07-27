# =============================================================================
# BULLETPROOF SOLUTION: Configuration Constants
# =============================================================================

# Create a new file: src/config/constants.py
"""
Configuration Constants and Defaults

This module defines all default values and expected types for configuration
settings. This provides a centralized, maintainable way to manage defaults
across the entire application.

Design Benefits:
- Single source of truth for all defaults
- Easy to update and maintain
- Type safety and validation
- Clear documentation of all settings
- Bulletproof fallback mechanisms
"""

from typing import Any, Type, Dict, Union


# =============================================================================
# CONFIGURATION DEFAULTS AND TYPES
# =============================================================================

class ConfigDefaults:
    """
    Centralized configuration defaults and type definitions.

    This class provides a clean, organized way to define all configuration
    defaults and their expected types. It can be easily extended and maintains
    a clear structure for all settings.
    """

    # Download Settings
    DOWNLOAD_SETTINGS = {
        'base_download_path': ('downloads', str),
        'chunk_size': (8192, int),
        'verify_downloads': (True, bool),
        'max_retries': (3, int),
        'timeout': (30, int),
        'skip_existing': (True, bool),
        'parallel_downloads': (4, int),
        'max_file_size_mb': (500, int),
        'retry_delay': (1.0, float),
        'allowed_extensions': ([], list),
        'blocked_extensions': (['.exe', '.bat', '.cmd', '.scr'], list)
    }

    # Canvas API Settings
    CANVAS_API = {
        'timeout': (30, int),
        'max_retries': (3, int),
        'retry_delay': (1.0, float),
        'rate_limit_delay': (0.1, float),
        'verify_ssl': (True, bool),
        'user_agent': ('Canvas-Downloader/1.0.0', str)
    }

    # Content Types Settings
    CONTENT_TYPES = {
        'announcements': ({'enabled': True, 'priority': 1}, dict),
        'assignments': ({'enabled': True, 'priority': 2}, dict),
        'discussions': ({'enabled': True, 'priority': 3}, dict),
        'files': ({'enabled': True, 'priority': 4}, dict),
        'modules': ({'enabled': True, 'priority': 5}, dict),
        'quizzes': ({'enabled': True, 'priority': 6}, dict),
        'grades': ({'enabled': True, 'priority': 7}, dict),
        'people': ({'enabled': False, 'priority': 8}, dict),
        'chat': ({'enabled': False, 'priority': 9}, dict)
    }

    # Folder Structure Settings
    FOLDER_STRUCTURE = {
        'organize_by_semester': (True, bool),
        'create_assignment_groups': (True, bool),
        'include_due_dates': (True, bool),
        'sanitize_names': (True, bool),
        'max_folder_depth': (10, int),
        'folder_name_template': ('{course_code}-{course_name}', str)
    }

    # Paths Settings
    PATHS = {
        'downloads_folder': ('downloads', str),
        'config_folder': ('config', str),
        'logs_folder': ('logs', str),
        'temp_folder': ('temp', str),
        'cache_folder': ('cache', str)
    }

    @classmethod
    def get_default_and_type(cls, section: str, key: str) -> tuple[Any, Type]:
        """
        Get default value and expected type for a configuration setting.

        Args:
            section: Configuration section name
            key: Configuration key name

        Returns:
            Tuple of (default_value, expected_type)
        """
        section_map = {
            'download_settings': cls.DOWNLOAD_SETTINGS,
            'canvas_api': cls.CANVAS_API,
            'content_types': cls.CONTENT_TYPES,
            'folder_structure': cls.FOLDER_STRUCTURE,
            'paths': cls.PATHS
        }

        if section in section_map and key in section_map[section]:
            return section_map[section][key]

        # Fallback for unknown settings
        return (None, str)

    @classmethod
    def get_default(cls, section: str, key: str) -> Any:
        """Get just the default value for a setting."""
        default, _ = cls.get_default_and_type(section, key)
        return default

    @classmethod
    def get_type(cls, section: str, key: str) -> Type:
        """Get just the expected type for a setting."""
        _, expected_type = cls.get_default_and_type(section, key)
        return expected_type


# =============================================================================
# SMART CONFIGURATION ACCESSOR
# =============================================================================

def get_config_default_and_type(key_path: str) -> tuple[Any, Type]:
    """
    Smart function to get default value and type for any configuration path.

    Args:
        key_path: Configuration path like 'download_settings.chunk_size'

    Returns:
        Tuple of (default_value, expected_type)

    Example:
        default, type_class = get_config_default_and_type('download_settings.chunk_size')
        # Returns: (8192, int)
    """
    try:
        parts = key_path.split('.')
        if len(parts) >= 2:
            section = parts[0]
            key = parts[1]
            return ConfigDefaults.get_default_and_type(section, key)
    except Exception:
        pass

    # Safe fallback
    return (None, str)
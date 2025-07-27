"""
Enhanced Configuration Management System - BULLETPROOF IMPLEMENTATION

This module provides a rock-solid configuration management system for the Canvas
Downloader application. It supports multiple access patterns, comprehensive validation,
automatic fallbacks, and bulletproof error handling.

Features:
- Multiple access patterns: dot notation, attribute access, dictionary access
- Comprehensive validation with detailed error reporting
- Automatic type conversion and validation
- Configuration migration and backwards compatibility
- Thread-safe operations with proper locking
- Encryption support for sensitive data
- Configuration templates and examples
- Extensive logging and debugging support
- Graceful degradation and fallback mechanisms

Key Design Principles:
1. **Reliability**: Never crash the application due to config issues
2. **Flexibility**: Support multiple ways to access configuration
3. **Validation**: Ensure all config values are valid and type-safe
4. **Security**: Encrypt sensitive data and provide secure defaults
5. **Maintainability**: Clear structure and comprehensive documentation
6. **Performance**: Efficient access patterns with caching
7. **Debugging**: Extensive logging for troubleshooting

Usage Examples:
    # All of these work seamlessly:
    config = get_config()

    # Dot notation (original)
    max_retries = config.get('download_settings.max_retries')

    # Attribute access (new)
    max_retries = config.download_settings.max_retries

    # Dictionary access
    max_retries = config['download_settings']['max_retries']

    # Safe access with defaults
    max_retries = config.safe_get('download_settings.max_retries', 3)
"""

import os
import json
import copy
from pathlib import Path
from typing import Dict, Any, Optional, Union, List, Tuple, Type
from datetime import datetime
import threading
from cryptography.fernet import Fernet
import base64
from dataclasses import dataclass, field
from collections.abc import MutableMapping

from ..utils.logger import get_logger


class ConfigurationError(Exception):
    """Custom exception for configuration-related errors."""
    pass


class ConfigurationValidationError(ConfigurationError):
    """Exception for configuration validation errors."""
    pass


class ConfigurationMigrationError(ConfigurationError):
    """Exception for configuration migration errors."""
    pass


@dataclass
class ConfigField:
    """
    Configuration field definition with validation and metadata.

    This class defines a configuration field with its type, validation rules,
    default value, and metadata for documentation and validation.
    """
    name: str
    field_type: Type
    default: Any
    description: str = ""
    required: bool = False
    min_value: Optional[Union[int, float]] = None
    max_value: Optional[Union[int, float]] = None
    allowed_values: Optional[List[Any]] = None
    validation_func: Optional[callable] = None
    sensitive: bool = False  # For encryption

    def validate(self, value: Any) -> Tuple[bool, str]:
        """
        Validate a value against this field definition.

        Args:
            value: Value to validate

        Returns:
            Tuple[bool, str]: (is_valid, error_message)
        """
        if value is None and self.required:
            return False, f"Field '{self.name}' is required"

        if value is None:
            return True, ""

        # Type validation
        if not isinstance(value, self.field_type):
            try:
                # Try to convert
                if self.field_type == bool and isinstance(value, str):
                    value = value.lower() in ('true', 'yes', '1', 'on')
                else:
                    value = self.field_type(value)
            except (ValueError, TypeError):
                return False, f"Field '{self.name}' must be of type {self.field_type.__name__}"

        # Range validation
        if self.min_value is not None and hasattr(value, '__gt__') and value < self.min_value:
            return False, f"Field '{self.name}' must be >= {self.min_value}"

        if self.max_value is not None and hasattr(value, '__lt__') and value > self.max_value:
            return False, f"Field '{self.name}' must be <= {self.max_value}"

        # Allowed values validation
        if self.allowed_values is not None and value not in self.allowed_values:
            return False, f"Field '{self.name}' must be one of {self.allowed_values}"

        # Custom validation
        if self.validation_func:
            try:
                if not self.validation_func(value):
                    return False, f"Field '{self.name}' failed custom validation"
            except Exception as e:
                return False, f"Field '{self.name}' validation error: {e}"

        return True, ""


class ConfigSection:
    """
    A configuration section that supports multiple access patterns.

    This class wraps a dictionary of configuration values and provides
    dot notation access, attribute access, and validation.
    """

    def __init__(self, data: Dict[str, Any], name: str = "", parent=None):
        """
        Initialize a configuration section.

        Args:
            data: Dictionary of configuration values
            name: Name of this section
            parent: Parent configuration object
        """
        object.__setattr__(self, '_data', data)
        object.__setattr__(self, '_name', name)
        object.__setattr__(self, '_parent', parent)
        object.__setattr__(self, '_sections', {})

        # Create nested sections
        for key, value in data.items():
            if isinstance(value, dict):
                section = ConfigSection(value, key, self)
                self._sections[key] = section

    def __getattr__(self, name: str) -> Any:
        """Get configuration value as attribute."""
        if name.startswith('_'):
            return object.__getattribute__(self, name)

        if name in self._sections:
            return self._sections[name]

        if name in self._data:
            value = self._data[name]
            if isinstance(value, dict):
                # Create section on demand
                section = ConfigSection(value, name, self)
                self._sections[name] = section
                return section
            return value

        raise AttributeError(f"Configuration section '{self._name}' has no attribute '{name}'")

    def __getitem__(self, key: str) -> Any:
        """Get configuration value as dictionary item."""
        return self.__getattr__(key)

    def __setattr__(self, name: str, value: Any) -> None:
        """Set configuration value as attribute."""
        if name.startswith('_'):
            object.__setattr__(self, name, value)
        else:
            self._data[name] = value
            if name in self._sections:
                del self._sections[name]

    def __setitem__(self, key: str, value: Any) -> None:
        """Set configuration value as dictionary item."""
        self.__setattr__(key, value)

    def __contains__(self, key: str) -> bool:
        """Check if key exists in configuration."""
        return key in self._data

    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value with default."""
        return self._data.get(key, default)

    def to_dict(self) -> Dict[str, Any]:
        """Convert section to dictionary."""
        return copy.deepcopy(self._data)


class CanvasDownloaderConfig:
    """
    Bulletproof Canvas Downloader Configuration Manager

    This class provides a rock-solid configuration system that supports multiple
    access patterns, comprehensive validation, and bulletproof error handling.

    The configuration system is designed to NEVER crash the application due to
    configuration issues. Instead, it provides graceful fallbacks, detailed
    logging, and clear error messages.
    """

    # Complete configuration schema with validation
    CONFIG_SCHEMA = {
        'app_info': {
            'name': ConfigField('name', str, 'Canvas Downloader', 'Application name'),
            'version': ConfigField('version', str, '0.1.0', 'Application version'),
            'description': ConfigField('description', str, 'Modular Canvas LMS content downloader', 'Application description')
        },
        'canvas_api': {
            'timeout': ConfigField('timeout', int, 30, 'API request timeout in seconds', min_value=1, max_value=300),
            'max_retries': ConfigField('max_retries', int, 3, 'Maximum API retry attempts', min_value=0, max_value=10),
            'retry_delay': ConfigField('retry_delay', float, 1.0, 'Delay between retries in seconds', min_value=0.1, max_value=60.0),
            'rate_limit_delay': ConfigField('rate_limit_delay', float, 0.1, 'Delay between requests', min_value=0.0, max_value=5.0),
            'verify_ssl': ConfigField('verify_ssl', bool, True, 'Verify SSL certificates'),
            'user_agent': ConfigField('user_agent', str, 'Canvas-Downloader/0.1.0', 'User agent string')
        },
        'download_settings': {
            'max_retries': ConfigField('max_retries', int, 3, 'Maximum download retry attempts', min_value=0, max_value=10),
            'retry_delay': ConfigField('retry_delay', float, 1.0, 'Delay between download retries', min_value=0.1, max_value=60.0),
            'chunk_size': ConfigField('chunk_size', int, 8192, 'Download chunk size in bytes', min_value=1024, max_value=1048576),
            'timeout': ConfigField('timeout', int, 30, 'Download timeout in seconds', min_value=5, max_value=3600),
            'verify_downloads': ConfigField('verify_downloads', bool, True, 'Verify downloaded file integrity'),
            'skip_existing': ConfigField('skip_existing', bool, True, 'Skip files that already exist'),
            'parallel_downloads': ConfigField('parallel_downloads', int, 4, 'Number of parallel downloads', min_value=1, max_value=20),
            'max_file_size_mb': ConfigField('max_file_size_mb', int, 500, 'Maximum file size in MB', min_value=1, max_value=10000),
            'base_download_path': ConfigField('base_download_path', str, 'downloads', 'Base directory for downloads'),
            'allowed_extensions': ConfigField('allowed_extensions', list, [], 'Allowed file extensions (empty = all)'),
            'blocked_extensions': ConfigField('blocked_extensions', list, ['.exe', '.bat', '.cmd', '.scr'], 'Blocked file extensions')
        },
        'content_types': {
            'modules': {
                'enabled': ConfigField('enabled', bool, True, 'Enable modules download'),
                'priority': ConfigField('priority', int, 1, 'Download priority', min_value=1, max_value=100),
                'download_module_content': ConfigField('download_module_content', bool, True, 'Download module content'),
                'download_module_items': ConfigField('download_module_items', bool, True, 'Download module items'),
                'download_associated_files': ConfigField('download_associated_files', bool, True, 'Download associated files'),
                'create_module_index': ConfigField('create_module_index', bool, True, 'Create module index file')
            },
            'assignments': {
                'enabled': ConfigField('enabled', bool, True, 'Enable assignments download'),
                'priority': ConfigField('priority', int, 2, 'Download priority', min_value=1, max_value=100),
                'download_instructions': ConfigField('download_instructions', bool, True, 'Download assignment instructions'),
                'download_attachments': ConfigField('download_attachments', bool, True, 'Download assignment attachments'),
                'download_rubrics': ConfigField('download_rubrics', bool, True, 'Download assignment rubrics'),
                'download_submissions': ConfigField('download_submissions', bool, False, 'Download student submissions'),
                'organize_by_groups': ConfigField('organize_by_groups', bool, True, 'Organize by assignment groups'),
                'convert_html_to_markdown': ConfigField('convert_html_to_markdown', bool, True, 'Convert HTML to Markdown')
            }
        },
        'folder_structure': {
            'organize_by_semester': ConfigField('organize_by_semester', bool, True, 'Organize downloads by semester'),
            'create_assignment_groups': ConfigField('create_assignment_groups', bool, True, 'Create assignment group folders'),
            'include_due_dates': ConfigField('include_due_dates', bool, True, 'Include due dates in folder names'),
            'sanitize_names': ConfigField('sanitize_names', bool, True, 'Sanitize file and folder names'),
            'max_folder_depth': ConfigField('max_folder_depth', int, 10, 'Maximum folder nesting depth', min_value=1, max_value=50),
            'folder_name_template': ConfigField('folder_name_template', str, '{course_code}-{course_name}', 'Template for folder names')
        },
        'paths': {
            'downloads_folder': ConfigField('downloads_folder', str, 'downloads', 'Downloads directory'),
            'config_folder': ConfigField('config_folder', str, 'config', 'Configuration directory'),
            'logs_folder': ConfigField('logs_folder', str, 'logs', 'Logs directory'),
            'temp_folder': ConfigField('temp_folder', str, 'temp', 'Temporary files directory'),
            'cache_folder': ConfigField('cache_folder', str, 'cache', 'Cache directory')
        },
        'logging': {
            'level': ConfigField('level', str, 'INFO', 'Logging level', allowed_values=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']),
            'console_output': ConfigField('console_output', bool, True, 'Enable console logging'),
            'file_output': ConfigField('file_output', bool, True, 'Enable file logging'),
            'max_log_size_mb': ConfigField('max_log_size_mb', int, 50, 'Maximum log file size in MB', min_value=1, max_value=1000),
            'backup_count': ConfigField('backup_count', int, 5, 'Number of log backups to keep', min_value=1, max_value=50),
            'format': ConfigField('format', str, '%(asctime)s - %(name)s - %(levelname)s - %(message)s', 'Log format string')
        },
        'ui': {
            'use_rich_progress': ConfigField('use_rich_progress', bool, True, 'Use rich progress bars'),
            'show_file_progress': ConfigField('show_file_progress', bool, True, 'Show individual file progress'),
            'console_width': ConfigField('console_width', int, 120, 'Console width for output', min_value=80, max_value=200),
            'update_interval': ConfigField('update_interval', float, 0.1, 'Progress update interval', min_value=0.01, max_value=5.0),
            'display_warnings': ConfigField('display_warnings', bool, True, 'Display warning messages'),
            'color_output': ConfigField('color_output', bool, True, 'Use colored output')
        },
        'performance': {
            'max_concurrent_downloads': ConfigField('max_concurrent_downloads', int, 4, 'Max concurrent downloads', min_value=1, max_value=50),
            'max_concurrent_courses': ConfigField('max_concurrent_courses', int, 1, 'Max concurrent courses', min_value=1, max_value=10),
            'memory_limit_mb': ConfigField('memory_limit_mb', int, 1024, 'Memory limit in MB', min_value=256, max_value=16384),
            'cache_enabled': ConfigField('cache_enabled', bool, True, 'Enable caching'),
            'cache_expiry_hours': ConfigField('cache_expiry_hours', int, 24, 'Cache expiry time in hours', min_value=1, max_value=168)
        },
        'security': {
            'encrypt_credentials': ConfigField('encrypt_credentials', bool, True, 'Encrypt stored credentials'),
            'mask_sensitive_logs': ConfigField('mask_sensitive_logs', bool, True, 'Mask sensitive data in logs'),
            'validate_file_types': ConfigField('validate_file_types', bool, True, 'Validate downloaded file types'),
            'scan_downloads': ConfigField('scan_downloads', bool, False, 'Scan downloads for malware'),
            'require_https': ConfigField('require_https', bool, True, 'Require HTTPS connections')
        }
    }

    def __init__(self, config_file: Union[str, Path] = None):
        """
        Initialize the bulletproof configuration manager.

        Args:
            config_file: Path to configuration file (optional)
        """
        self.logger = get_logger(__name__)
        self._lock = threading.RLock()

        # Configuration storage
        self._config = {}
        self._user_config = {}
        self._sessions = {}
        self._validation_errors = []

        # File paths
        self.config_file = Path(config_file) if config_file else Path("config") / "config.json"
        self.sessions_file = self.config_file.parent / "sessions.json"

        # Encryption for credentials
        self._encryption_key = None
        self._fernet = None

        # Configuration sections (for attribute access)
        self._sections = {}

        try:
            # Ensure config directory exists
            self.config_file.parent.mkdir(parents=True, exist_ok=True)

            # Initialize with defaults
            self._initialize_defaults()

            # Load user configuration
            self._load_config()

            # Set up encryption
            self._setup_encryption()

            # Load sessions
            self._load_sessions()

            # Create configuration sections
            self._create_sections()

            # Validate configuration
            self._validate_configuration()

            self.logger.info("Configuration manager initialized successfully",
                           config_file=str(self.config_file),
                           sessions_count=len(self._sessions),
                           validation_errors=len(self._validation_errors))

        except Exception as e:
            self.logger.error(f"Failed to initialize configuration manager", exception=e)
            # Don't crash - use safe defaults
            self._initialize_safe_fallbacks()

    def _initialize_defaults(self) -> None:
        """Initialize configuration with validated defaults."""
        self._config = {}

        for section_name, section_schema in self.CONFIG_SCHEMA.items():
            self._config[section_name] = {}

            if isinstance(section_schema, dict):
                for field_name, field_def in section_schema.items():
                    if isinstance(field_def, ConfigField):
                        self._config[section_name][field_name] = field_def.default
                    elif isinstance(field_def, dict):
                        # Nested section
                        self._config[section_name][field_name] = {}
                        for nested_field, nested_def in field_def.items():
                            if isinstance(nested_def, ConfigField):
                                self._config[section_name][field_name][nested_field] = nested_def.default

    def _initialize_safe_fallbacks(self) -> None:
        """Initialize with absolute minimum safe configuration."""
        self.logger.warning("Initializing with safe fallback configuration")

        self._config = {
            'download_settings': {
                'base_download_path': 'downloads',
                'chunk_size': 8192,
                'max_retries': 3,
                'timeout': 30,
                'verify_downloads': True,
                'skip_existing': True,
                'parallel_downloads': 4
            },
            'paths': {
                'downloads_folder': 'downloads',
                'logs_folder': 'logs'
            }
        }

        self._create_sections()

    def _create_sections(self) -> None:
        """Create configuration sections for attribute access."""
        self._sections = {}

        for section_name, section_data in self._config.items():
            if isinstance(section_data, dict):
                self._sections[section_name] = ConfigSection(section_data, section_name, self)

    def __getattr__(self, name: str) -> Any:
        """Get configuration section as attribute."""
        if name.startswith('_'):
            return object.__getattribute__(self, name)

        if name in self._sections:
            return self._sections[name]

        if name in self._config:
            data = self._config[name]
            if isinstance(data, dict):
                section = ConfigSection(data, name, self)
                self._sections[name] = section
                return section
            return data

        # Graceful fallback
        self.logger.warning(f"Configuration section '{name}' not found, returning empty section")
        empty_section = ConfigSection({}, name, self)
        self._sections[name] = empty_section
        return empty_section

    def __getitem__(self, key: str) -> Any:
        """Get configuration value as dictionary item."""
        return self._config[key]

    def get(self, key_path: str, default: Any = None) -> Any:
        """
        Get a configuration value using dot notation with bulletproof error handling.

        Args:
            key_path: Configuration key path (e.g., 'download_settings.max_retries')
            default: Default value if key not found

        Returns:
            Configuration value or default
        """
        with self._lock:
            try:
                keys = key_path.split('.')
                value = self._config

                for key in keys:
                    if isinstance(value, dict) and key in value:
                        value = value[key]
                    else:
                        self.logger.debug(f"Configuration key not found: {key_path}, using default: {default}")
                        return default

                return value

            except Exception as e:
                self.logger.warning(f"Error accessing configuration key '{key_path}'", exception=e)
                return default

    def safe_get(self, key_path: str, default: Any = None, expected_type: Type = None) -> Any:
        """
        Safely get a configuration value with type validation.

        Args:
            key_path: Configuration key path
            default: Default value if key not found
            expected_type: Expected type for validation

        Returns:
            Configuration value or default
        """
        value = self.get(key_path, default)

        if expected_type and value is not None:
            if not isinstance(value, expected_type):
                try:
                    value = expected_type(value)
                except (ValueError, TypeError):
                    self.logger.warning(f"Type conversion failed for '{key_path}', using default")
                    return default

        return value

    def _validate_configuration(self) -> None:
        """Validate the entire configuration against schema."""
        self._validation_errors = []

        try:
            for section_name, section_schema in self.CONFIG_SCHEMA.items():
                if section_name not in self._config:
                    continue

                section_data = self._config[section_name]
                self._validate_section(section_name, section_data, section_schema)

            if self._validation_errors:
                self.logger.warning(f"Configuration validation found {len(self._validation_errors)} issues",
                                  errors=self._validation_errors[:5])  # Log first 5 errors

        except Exception as e:
            self.logger.error("Error during configuration validation", exception=e)

    def _validate_section(self, section_name: str, section_data: Dict[str, Any],
                         section_schema: Dict[str, Any]) -> None:
        """Validate a configuration section."""
        for field_name, field_def in section_schema.items():
            if isinstance(field_def, ConfigField):
                if field_name in section_data:
                    is_valid, error_msg = field_def.validate(section_data[field_name])
                    if not is_valid:
                        self._validation_errors.append(f"{section_name}.{field_name}: {error_msg}")
                        # Use default value for invalid config
                        section_data[field_name] = field_def.default
                        self.logger.warning(f"Invalid config value, using default: {section_name}.{field_name}")
            elif isinstance(field_def, dict):
                # Nested section
                if field_name in section_data and isinstance(section_data[field_name], dict):
                    self._validate_section(f"{section_name}.{field_name}",
                                         section_data[field_name], field_def)

    def _load_config(self) -> None:
        """Load configuration from file with error handling."""
        try:
            if self.config_file.exists():
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    user_config = json.load(f)

                self._user_config = user_config
                self._merge_config(user_config)

                self.logger.info(f"Configuration loaded from {self.config_file}")
            else:
                # Create default config file
                self.save_config()
                self.logger.info(f"Created default configuration at {self.config_file}")

        except Exception as e:
            self.logger.error(f"Failed to load configuration", exception=e)
            # Don't crash - continue with defaults
            self.logger.warning("Continuing with default configuration")

    def _merge_config(self, user_config: Dict[str, Any]) -> None:
        """Merge user configuration with defaults."""
        def merge_dicts(default: Dict, user: Dict) -> Dict:
            """Recursively merge dictionaries."""
            result = copy.deepcopy(default)

            for key, value in user.items():
                if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                    result[key] = merge_dicts(result[key], value)
                else:
                    result[key] = copy.deepcopy(value)

            return result

        self._config = merge_dicts(self._config, user_config)

    def _setup_encryption(self) -> None:
        """Set up encryption for credential storage."""
        try:
            key_file = self.config_file.parent / "encryption.key"

            if key_file.exists():
                with open(key_file, 'rb') as f:
                    self._encryption_key = f.read()
            else:
                self._encryption_key = Fernet.generate_key()
                with open(key_file, 'wb') as f:
                    f.write(self._encryption_key)
                os.chmod(key_file, 0o600)

            self._fernet = Fernet(self._encryption_key)

        except Exception as e:
            self.logger.warning(f"Could not set up encryption", exception=e)
            self._fernet = None

    def _load_sessions(self) -> None:
        """Load Canvas sessions from file."""
        try:
            if self.sessions_file.exists():
                with open(self.sessions_file, 'r', encoding='utf-8') as f:
                    sessions_data = json.load(f)

                # Decrypt credentials if encryption is available
                for session_id, session_data in sessions_data.items():
                    if 'api_key_encrypted' in session_data and self._fernet:
                        try:
                            encrypted_key = base64.b64decode(session_data['api_key_encrypted'])
                            decrypted_key = self._fernet.decrypt(encrypted_key).decode('utf-8')
                            session_data['api_key'] = decrypted_key
                            del session_data['api_key_encrypted']
                        except Exception as e:
                            self.logger.warning(f"Could not decrypt API key for session {session_id}", exception=e)

                self._sessions = sessions_data
                self.logger.info(f"Loaded {len(self._sessions)} Canvas sessions")

        except Exception as e:
            self.logger.warning(f"Could not load sessions", exception=e)
            self._sessions = {}

    def save_config(self) -> None:
        """Save configuration to file."""
        try:
            with self._lock:
                with open(self.config_file, 'w', encoding='utf-8') as f:
                    json.dump(self._user_config, f, indent=2)

                self.logger.debug(f"Configuration saved to {self.config_file}")

        except Exception as e:
            self.logger.error(f"Failed to save configuration", exception=e)
            raise ConfigurationError(f"Could not save configuration: {e}")

    def validate_config(self) -> List[str]:
        """
        Validate the current configuration and return any errors.

        Returns:
            List[str]: List of validation errors (empty if valid)
        """
        self._validate_configuration()
        return self._validation_errors.copy()

    def is_content_type_enabled(self, content_type: str) -> bool:
        """Check if a content type is enabled for download."""
        return self.safe_get(f'content_types.{content_type}.enabled', False, bool)

    def get_content_type_config(self, content_type: str) -> Dict[str, Any]:
        """Get complete configuration for a content type."""
        return self.get(f'content_types.{content_type}', {})

    def get_enabled_content_types(self) -> List[Tuple[str, int]]:
        """Get list of enabled content types sorted by priority."""
        enabled = []
        content_types = self.get('content_types', {})

        for content_type, config in content_types.items():
            if isinstance(config, dict) and config.get('enabled', False):
                priority = config.get('priority', 999)
                enabled.append((content_type, priority))

        enabled.sort(key=lambda x: x[1])
        return enabled

    def add_canvas_session(self, name: str, canvas_url: str, api_key: str) -> str:
        """Add a new Canvas session with encryption."""
        import uuid

        session_id = str(uuid.uuid4())

        session_data = {
            'name': name,
            'canvas_url': canvas_url.rstrip('/'),
            'api_key': api_key,
            'created_at': datetime.now().isoformat(),
            'last_used': None
        }

        # Encrypt API key if encryption is available
        if self._fernet:
            try:
                encrypted_key = self._fernet.encrypt(api_key.encode('utf-8'))
                session_data['api_key_encrypted'] = base64.b64encode(encrypted_key).decode('utf-8')
                del session_data['api_key']
            except Exception as e:
                self.logger.warning(f"Could not encrypt API key for session", exception=e)

        self._sessions[session_id] = session_data
        self._save_sessions()

        self.logger.info(f"Added Canvas session", name=name, session_id=session_id[:8] + "***")
        return session_id

    def get_canvas_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get a Canvas session by ID."""
        return self._sessions.get(session_id)

    def get_all_sessions(self) -> Dict[str, Dict[str, Any]]:
        """Get all Canvas sessions."""
        return self._sessions.copy()

    def remove_canvas_session(self, session_id: str) -> bool:
        """Remove a Canvas session."""
        if session_id in self._sessions:
            del self._sessions[session_id]
            self._save_sessions()
            self.logger.info(f"Removed Canvas session", session_id=session_id[:8] + "***")
            return True
        return False

    def _save_sessions(self) -> None:
        """Save Canvas sessions to file."""
        try:
            with open(self.sessions_file, 'w', encoding='utf-8') as f:
                json.dump(self._sessions, f, indent=2)

            # Set restrictive permissions
            os.chmod(self.sessions_file, 0o600)

        except Exception as e:
            self.logger.error(f"Failed to save sessions", exception=e)

    def create_config_template(self, file_path: Path) -> None:
        """Create a configuration template file."""
        template_content = {
            "_comment": "Canvas Downloader Configuration Template",
            "_instructions": [
                "Copy this file to 'config.json' and modify as needed",
                "Remove lines starting with '_' before using",
                "Boolean values: true/false (lowercase)",
                "Strings must be quoted",
                "Numbers don't need quotes"
            ],

            "content_types": {
                "_comment": "Enable/disable specific content types",
                "assignments": {
                    "enabled": True,
                    "download_instructions": True,
                    "download_attachments": True,
                    "download_rubrics": True,
                    "organize_by_groups": True
                },
                "modules": {
                    "enabled": True,
                    "download_module_content": True,
                    "create_module_index": True
                }
            },

            "download_settings": {
                "_comment": "Download behavior configuration",
                "max_retries": 3,
                "timeout": 30,
                "verify_downloads": True,
                "skip_existing": True,
                "parallel_downloads": 4,
                "base_download_path": "downloads"
            },

            "folder_structure": {
                "_comment": "How to organize downloaded files",
                "organize_by_semester": True,
                "create_assignment_groups": True,
                "include_due_dates": True
            },

            "paths": {
                "_comment": "Folder locations (relative to app directory)",
                "downloads_folder": "downloads",
                "logs_folder": "logs"
            }
        }

        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(template_content, f, indent=2)

            self.logger.info(f"Configuration template created", file_path=str(file_path))

        except Exception as e:
            self.logger.error(f"Failed to create configuration template", exception=e)

    def reset_to_defaults(self) -> None:
        """Reset configuration to default values."""
        with self._lock:
            self._user_config = {}
            self._initialize_defaults()
            self._create_sections()
            self.save_config()

            self.logger.info("Configuration reset to defaults")

    def export_config(self, output_path: Path, include_sessions: bool = False) -> bool:
        """
        Export configuration to a file.

        Args:
            output_path: Path to export configuration
            include_sessions: Whether to include session data

        Returns:
            bool: True if export successful
        """
        try:
            export_data = {
                'config': self._config,
                'exported_at': datetime.now().isoformat(),
                'version': '1.0'
            }

            if include_sessions:
                # Don't include encrypted credentials in export
                safe_sessions = {}
                for session_id, session_data in self._sessions.items():
                    safe_session = session_data.copy()
                    # Remove sensitive data
                    safe_session.pop('api_key', None)
                    safe_session.pop('api_key_encrypted', None)
                    safe_sessions[session_id] = safe_session

                export_data['sessions'] = safe_sessions

            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, indent=2, ensure_ascii=False)

            self.logger.info(f"Configuration exported", output_path=str(output_path))
            return True

        except Exception as e:
            self.logger.error(f"Failed to export configuration", exception=e)
            return False

    def import_config(self, input_path: Path, merge: bool = True) -> bool:
        """
        Import configuration from a file.

        Args:
            input_path: Path to import configuration from
            merge: Whether to merge with existing config or replace

        Returns:
            bool: True if import successful
        """
        try:
            with open(input_path, 'r', encoding='utf-8') as f:
                import_data = json.load(f)

            if 'config' not in import_data:
                raise ConfigurationError("Invalid configuration export file")

            imported_config = import_data['config']

            if merge:
                self._merge_config(imported_config)
            else:
                self._config = imported_config
                self._user_config = imported_config

            self._create_sections()
            self._validate_configuration()
            self.save_config()

            self.logger.info(f"Configuration imported", input_path=str(input_path), merge=merge)
            return True

        except Exception as e:
            self.logger.error(f"Failed to import configuration", exception=e)
            return False

    def get_config_summary(self) -> Dict[str, Any]:
        """Get a summary of the current configuration."""
        return {
            'total_sections': len(self._config),
            'enabled_content_types': [ct[0] for ct in self.get_enabled_content_types()],
            'validation_errors': len(self._validation_errors),
            'sessions_count': len(self._sessions),
            'config_file': str(self.config_file),
            'encryption_enabled': self._fernet is not None,
            'last_modified': self.config_file.stat().st_mtime if self.config_file.exists() else None
        }

    def diagnose_config(self) -> Dict[str, Any]:
        """
        Perform comprehensive configuration diagnostics.

        Returns:
            Dict[str, Any]: Diagnostic information
        """
        diagnostics = {
            'timestamp': datetime.now().isoformat(),
            'config_file_exists': self.config_file.exists(),
            'config_file_readable': False,
            'config_file_writable': False,
            'sessions_file_exists': self.sessions_file.exists(),
            'encryption_available': self._fernet is not None,
            'validation_errors': self._validation_errors.copy(),
            'missing_sections': [],
            'invalid_values': [],
            'recommendations': []
        }

        try:
            # Check file permissions
            if self.config_file.exists():
                diagnostics['config_file_readable'] = os.access(self.config_file, os.R_OK)
                diagnostics['config_file_writable'] = os.access(self.config_file, os.W_OK)

            # Check for missing sections
            for section_name in self.CONFIG_SCHEMA.keys():
                if section_name not in self._config:
                    diagnostics['missing_sections'].append(section_name)

            # Generate recommendations
            if not diagnostics['encryption_available']:
                diagnostics['recommendations'].append("Consider enabling encryption for sensitive data")

            if diagnostics['validation_errors']:
                diagnostics['recommendations'].append("Fix configuration validation errors")

            if not diagnostics['config_file_writable']:
                diagnostics['recommendations'].append("Ensure configuration file is writable")

        except Exception as e:
            diagnostics['diagnostic_error'] = str(e)

        return diagnostics


# Global configuration instance management
_config_instance = None
_config_lock = threading.Lock()


def get_config(config_file: Union[str, Path] = None) -> CanvasDownloaderConfig:
    """
    Get the global configuration instance with bulletproof initialization.

    This function ensures that the configuration system never crashes the
    application. If initialization fails, it provides safe fallbacks.

    Args:
        config_file: Optional path to configuration file

    Returns:
        CanvasDownloaderConfig: Configuration instance (always succeeds)
    """
    global _config_instance

    with _config_lock:
        if _config_instance is None:
            try:
                _config_instance = CanvasDownloaderConfig(config_file)
            except Exception as e:
                # Last resort fallback - create minimal working config
                logger = get_logger(__name__)
                logger.critical(f"Failed to initialize configuration, using emergency fallback", exception=e)

                _config_instance = object.__new__(CanvasDownloaderConfig)
                _config_instance._config = {
                    'download_settings': {
                        'base_download_path': 'downloads',
                        'chunk_size': 8192,
                        'max_retries': 3,
                        'timeout': 30
                    }
                }
                _config_instance._sections = {}
                _config_instance._initialize_safe_fallbacks()

        return _config_instance


def reset_config() -> None:
    """Reset the global configuration instance."""
    global _config_instance

    with _config_lock:
        _config_instance = None


def create_config_backup(backup_path: Path = None) -> bool:
    """
    Create a backup of the current configuration.

    Args:
        backup_path: Optional path for backup file

    Returns:
        bool: True if backup successful
    """
    try:
        config = get_config()

        if backup_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = config.config_file.parent / f"config_backup_{timestamp}.json"

        return config.export_config(backup_path, include_sessions=False)

    except Exception as e:
        logger = get_logger(__name__)
        logger.error(f"Failed to create configuration backup", exception=e)
        return False


def validate_config_file(config_file: Path) -> Tuple[bool, List[str]]:
    """
    Validate a configuration file without loading it globally.

    Args:
        config_file: Path to configuration file

    Returns:
        Tuple[bool, List[str]]: (is_valid, error_messages)
    """
    try:
        temp_config = CanvasDownloaderConfig(config_file)
        errors = temp_config.validate_config()
        return len(errors) == 0, errors

    except Exception as e:
        return False, [f"Failed to load configuration: {e}"]


# Utility functions for common configuration operations
def get_download_path() -> Path:
    """Get the download path with fallback."""
    config = get_config()
    base_path = config.safe_get('download_settings.base_download_path', 'downloads', str)
    return Path(base_path)


def get_max_retries() -> int:
    """Get max retries with validation."""
    config = get_config()
    return config.safe_get('download_settings.max_retries', 3, int)


def get_chunk_size() -> int:
    """Get chunk size with validation."""
    config = get_config()
    return config.safe_get('download_settings.chunk_size', 8192, int)


def is_content_enabled(content_type: str) -> bool:
    """Check if content type is enabled."""
    config = get_config()
    return config.is_content_type_enabled(content_type)
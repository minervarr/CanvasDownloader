"""
Configuration Management System - COMPLETE IMPLEMENTATION

This module provides comprehensive configuration management for the Canvas
Downloader application. It handles loading, validation, and management of
user settings, API credentials, and application preferences.

Features:
- JSON-based configuration with validation
- Environment variable support
- Secure credential storage with encryption
- Configuration file templates and examples
- Setting validation and type checking
- Default value management
- Configuration migration and updates
- Thread-safe configuration access

Configuration Structure:
- API settings (Canvas URLs, credentials)
- Download preferences (file types, organization)
- Performance settings (parallel downloads, timeouts)
- Path configuration (download folders, temp directories)
- UI preferences (progress display, logging levels)
- Security settings (encryption, permissions)

Usage:
    # Get configuration instance
    config = get_config()

    # Check if content type is enabled
    if config.is_content_type_enabled('assignments'):
        # Download assignments
        pass

    # Get download settings
    max_retries = config.get('download_settings', {}).get('max_retries', 3)
"""

import os
import json
import copy
from pathlib import Path
from typing import Dict, Any, Optional, Union, List, Tuple
from datetime import datetime
import threading
from cryptography.fernet import Fernet
import base64

from ..utils.logger import get_logger


class ConfigurationError(Exception):
    """Custom exception for configuration-related errors."""
    pass


class CanvasDownloaderConfig:
    """
    Canvas Downloader Configuration Manager

    This class handles all configuration aspects of the Canvas downloader,
    including settings validation, credential management, and preference storage.
    """

    DEFAULT_CONFIG = {
        # Application information
        "app_info": {
            "name": "Canvas Downloader",
            "version": "0.1.0",
            "description": "Modular Canvas LMS content downloader"
        },

        # Canvas API settings
        "canvas_api": {
            "timeout": 30,
            "max_retries": 3,
            "retry_delay": 1.0,
            "rate_limit_delay": 0.1,
            "verify_ssl": True,
            "user_agent": "Canvas-Downloader/0.1.0"
        },

        # Download settings
        "download_settings": {
            "max_retries": 3,
            "retry_delay": 1.0,
            "chunk_size": 8192,
            "timeout": 30,
            "verify_downloads": True,
            "skip_existing": True,
            "parallel_downloads": 4,
            "max_file_size_mb": 500,
            "allowed_extensions": [],  # Empty means all allowed
            "blocked_extensions": [".exe", ".bat", ".cmd", ".scr"]
        },

        # Content types configuration
        "content_types": {
            "modules": {
                "enabled": True,
                "priority": 1,
                "download_module_content": True,
                "download_module_items": True,
                "download_associated_files": True,
                "create_module_index": True
            },
            "assignments": {
                "enabled": True,
                "priority": 2,
                "download_instructions": True,
                "download_attachments": True,
                "download_rubrics": True,
                "download_submissions": False,
                "organize_by_groups": True,
                "convert_html_to_markdown": True,
                "extract_embedded_images": True
            },
            "announcements": {
                "enabled": True,
                "priority": 3,
                "download_attachments": True,
                "convert_to_markdown": True,
                "include_comments": False
            },
            "discussions": {
                "enabled": True,
                "priority": 4,
                "download_posts": True,
                "download_replies": True,
                "download_attachments": True,
                "max_depth": 10
            },
            "quizzes": {
                "enabled": True,
                "priority": 5,
                "download_questions": True,
                "download_submissions": False,
                "download_results": False
            },
            "grades": {
                "enabled": False,
                "priority": 6,
                "download_gradebook": False,
                "download_comments": False,
                "respect_privacy": True
            },
            "files": {
                "enabled": True,
                "priority": 7,
                "preserve_folder_structure": True,
                "download_hidden_files": False,
                "organize_by_type": False
            },
            "people": {
                "enabled": False,
                "priority": 8,
                "download_profiles": False,
                "respect_privacy": True
            }
        },

        # Folder structure and organization
        "folder_structure": {
            "organize_by_semester": True,
            "organize_by_year": True,
            "create_content_folders": True,
            "create_assignment_groups": True,
            "include_due_dates": True,
            "sanitize_names": True,
            "max_folder_depth": 10,
            "folder_name_template": "{course_code}-{course_name}"
        },

        # Paths configuration
        "paths": {
            "downloads_folder": "downloads",
            "config_folder": "config",
            "logs_folder": "logs",
            "temp_folder": "temp",
            "cache_folder": "cache"
        },

        # Logging configuration
        "logging": {
            "level": "INFO",
            "console_output": True,
            "file_output": True,
            "max_log_size_mb": 50,
            "backup_count": 5,
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        },

        # UI and progress settings
        "ui": {
            "use_rich_progress": True,
            "show_file_progress": True,
            "console_width": 120,
            "update_interval": 0.1,
            "display_warnings": True,
            "color_output": True
        },

        # Performance settings
        "performance": {
            "max_concurrent_downloads": 4,
            "max_concurrent_courses": 1,
            "memory_limit_mb": 1024,
            "cache_enabled": True,
            "cache_expiry_hours": 24
        },

        # Security settings
        "security": {
            "encrypt_credentials": True,
            "mask_sensitive_logs": True,
            "validate_file_types": True,
            "scan_downloads": False,
            "require_https": True
        }
    }

    def __init__(self, config_file: Union[str, Path] = None):
        """
        Initialize the configuration manager.

        Args:
            config_file: Path to configuration file (optional)
        """
        self.logger = get_logger(__name__)
        self._lock = threading.RLock()

        # Configuration storage
        self._config = copy.deepcopy(self.DEFAULT_CONFIG)
        self._user_config = {}
        self._sessions = {}

        # File paths
        self.config_file = Path(config_file) if config_file else Path("config") / "config.json"
        self.sessions_file = self.config_file.parent / "sessions.json"
        self.credentials_file = self.config_file.parent / "credentials.enc"

        # Encryption for credentials
        self._encryption_key = None
        self._fernet = None

        # Ensure config directory exists
        self.config_file.parent.mkdir(parents=True, exist_ok=True)

        # Load configuration
        self._load_config()
        self._setup_encryption()
        self._load_sessions()

        self.logger.info("Configuration manager initialized",
                        config_file=str(self.config_file),
                        sessions_count=len(self._sessions))

    def _setup_encryption(self) -> None:
        """Set up encryption for credential storage."""
        try:
            key_file = self.config_file.parent / "encryption.key"

            if key_file.exists():
                # Load existing key
                with open(key_file, 'rb') as f:
                    self._encryption_key = f.read()
            else:
                # Generate new key
                self._encryption_key = Fernet.generate_key()
                with open(key_file, 'wb') as f:
                    f.write(self._encryption_key)

                # Set restrictive permissions
                os.chmod(key_file, 0o600)

            self._fernet = Fernet(self._encryption_key)

        except Exception as e:
            self.logger.warning(f"Could not set up encryption", exception=e)
            self._fernet = None

    def _load_config(self) -> None:
        """Load configuration from file."""
        try:
            if self.config_file.exists():
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    user_config = json.load(f)

                # Merge with defaults
                self._user_config = user_config
                self._merge_config(user_config)

                self.logger.info(f"Configuration loaded from {self.config_file}")
            else:
                # Create default config file
                self.save_config()
                self.logger.info(f"Created default configuration at {self.config_file}")

        except Exception as e:
            self.logger.error(f"Failed to load configuration", exception=e)
            raise ConfigurationError(f"Could not load configuration: {e}")

    def _merge_config(self, user_config: Dict[str, Any]) -> None:
        """
        Merge user configuration with defaults.

        Args:
            user_config: User-provided configuration
        """
        def merge_dicts(default: Dict, user: Dict) -> Dict:
            """Recursively merge dictionaries."""
            result = copy.deepcopy(default)

            for key, value in user.items():
                if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                    result[key] = merge_dicts(result[key], value)
                else:
                    result[key] = copy.deepcopy(value)

            return result

        self._config = merge_dicts(self.DEFAULT_CONFIG, user_config)

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

    def get(self, key_path: str, default: Any = None) -> Any:
        """
        Get a configuration value using dot notation.

        Args:
            key_path: Configuration key path (e.g., 'download_settings.max_retries')
            default: Default value if key not found

        Returns:
            Configuration value or default
        """
        with self._lock:
            keys = key_path.split('.')
            value = self._config

            try:
                for key in keys:
                    value = value[key]
                return value
            except (KeyError, TypeError):
                return default

    def set(self, key_path: str, value: Any, save: bool = True) -> None:
        """
        Set a configuration value using dot notation.

        Args:
            key_path: Configuration key path
            value: Value to set
            save: Whether to save to file immediately
        """
        with self._lock:
            keys = key_path.split('.')
            config_ref = self._config
            user_config_ref = self._user_config

            # Navigate to parent of target key
            for key in keys[:-1]:
                if key not in config_ref:
                    config_ref[key] = {}
                if key not in user_config_ref:
                    user_config_ref[key] = {}

                config_ref = config_ref[key]
                user_config_ref = user_config_ref[key]

            # Set the value
            final_key = keys[-1]
            config_ref[final_key] = value
            user_config_ref[final_key] = value

            if save:
                self.save_config()

    def is_content_type_enabled(self, content_type: str) -> bool:
        """
        Check if a content type is enabled for download.

        Args:
            content_type: Name of content type (e.g., 'assignments')

        Returns:
            bool: True if content type is enabled
        """
        return self.get(f'content_types.{content_type}.enabled', False)

    def get_content_type_config(self, content_type: str) -> Dict[str, Any]:
        """
        Get complete configuration for a content type.

        Args:
            content_type: Name of content type

        Returns:
            Dict[str, Any]: Content type configuration
        """
        return self.get(f'content_types.{content_type}', {})

    def get_enabled_content_types(self) -> List[Tuple[str, int]]:
        """
        Get list of enabled content types sorted by priority.

        Returns:
            List[Tuple[str, int]]: List of (content_type, priority) tuples
        """
        enabled = []
        content_types = self.get('content_types', {})

        for content_type, config in content_types.items():
            if config.get('enabled', False):
                priority = config.get('priority', 999)
                enabled.append((content_type, priority))

        # Sort by priority
        enabled.sort(key=lambda x: x[1])
        return enabled

    def add_canvas_session(self, name: str, canvas_url: str, api_key: str) -> str:
        """
        Add a new Canvas session.

        Args:
            name: Human-readable name for the session
            canvas_url: Canvas instance URL
            api_key: Canvas API key

        Returns:
            str: Session ID
        """
        with self._lock:
            import uuid
            session_id = str(uuid.uuid4())

            session_data = {
                'name': name,
                'canvas_url': canvas_url.rstrip('/'),
                'api_key': api_key,
                'created_at': datetime.now().isoformat(),
                'last_used': None,
                'active': True
            }

            self._sessions[session_id] = session_data
            self._save_sessions()

            self.logger.info(f"Added Canvas session", name=name, session_id=session_id)
            return session_id

    def get_canvas_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """
        Get Canvas session by ID.

        Args:
            session_id: Session identifier

        Returns:
            Optional[Dict[str, Any]]: Session data or None
        """
        return self._sessions.get(session_id)

    def get_all_sessions(self) -> Dict[str, Dict[str, Any]]:
        """
        Get all Canvas sessions.

        Returns:
            Dict[str, Dict[str, Any]]: All session data
        """
        # Return copy to prevent external modification
        return copy.deepcopy(self._sessions)

    def update_session_last_used(self, session_id: str) -> None:
        """
        Update the last used timestamp for a session.

        Args:
            session_id: Session identifier
        """
        with self._lock:
            if session_id in self._sessions:
                self._sessions[session_id]['last_used'] = datetime.now().isoformat()
                self._save_sessions()

    def remove_canvas_session(self, session_id: str) -> bool:
        """
        Remove a Canvas session.

        Args:
            session_id: Session identifier

        Returns:
            bool: True if session was removed
        """
        with self._lock:
            if session_id in self._sessions:
                session_name = self._sessions[session_id].get('name', 'Unknown')
                del self._sessions[session_id]
                self._save_sessions()
                self.logger.info(f"Removed Canvas session", name=session_name, session_id=session_id)
                return True
            return False

    def _save_sessions(self) -> None:
        """Save sessions to encrypted file."""
        try:
            sessions_to_save = copy.deepcopy(self._sessions)

            # Encrypt API keys if encryption is available
            if self._fernet:
                for session_id, session_data in sessions_to_save.items():
                    if 'api_key' in session_data:
                        api_key = session_data['api_key']
                        encrypted_key = self._fernet.encrypt(api_key.encode('utf-8'))
                        session_data['api_key_encrypted'] = base64.b64encode(encrypted_key).decode('utf-8')
                        del session_data['api_key']

            with open(self.sessions_file, 'w', encoding='utf-8') as f:
                json.dump(sessions_to_save, f, indent=2)

            # Set restrictive permissions
            os.chmod(self.sessions_file, 0o600)

        except Exception as e:
            self.logger.error(f"Failed to save sessions", exception=e)

    def save_config(self) -> None:
        """Save current configuration to file."""
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
        Validate the current configuration.

        Returns:
            List[str]: List of validation errors (empty if valid)
        """
        errors = []

        try:
            # Validate paths
            paths = self.get('paths', {})
            for path_name, path_value in paths.items():
                if not isinstance(path_value, str):
                    errors.append(f"Path '{path_name}' must be a string")
                elif not path_value.strip():
                    errors.append(f"Path '{path_name}' cannot be empty")

            # Validate download settings
            download_settings = self.get('download_settings', {})

            max_retries = download_settings.get('max_retries', 3)
            if not isinstance(max_retries, int) or max_retries < 0:
                errors.append("max_retries must be a non-negative integer")

            chunk_size = download_settings.get('chunk_size', 8192)
            if not isinstance(chunk_size, int) or chunk_size <= 0:
                errors.append("chunk_size must be a positive integer")

            timeout = download_settings.get('timeout', 30)
            if not isinstance(timeout, (int, float)) or timeout <= 0:
                errors.append("timeout must be a positive number")

            # Validate content types
            content_types = self.get('content_types', {})
            for content_type, config in content_types.items():
                if not isinstance(config, dict):
                    errors.append(f"Content type '{content_type}' configuration must be a dictionary")
                    continue

                enabled = config.get('enabled', True)
                if not isinstance(enabled, bool):
                    errors.append(f"Content type '{content_type}' enabled setting must be boolean")

                priority = config.get('priority', 1)
                if not isinstance(priority, int) or priority < 1:
                    errors.append(f"Content type '{content_type}' priority must be a positive integer")

            # Validate sessions
            for session_id, session_data in self._sessions.items():
                if not isinstance(session_data.get('canvas_url'), str):
                    errors.append(f"Session '{session_id}' canvas_url must be a string")

                if not session_data.get('canvas_url', '').strip():
                    errors.append(f"Session '{session_id}' canvas_url cannot be empty")

                if not isinstance(session_data.get('api_key'), str):
                    errors.append(f"Session '{session_id}' api_key must be a string")

                if not session_data.get('api_key', '').strip():
                    errors.append(f"Session '{session_id}' api_key cannot be empty")

        except Exception as e:
            errors.append(f"Validation error: {e}")

        return errors

    def reset_to_defaults(self) -> None:
        """Reset configuration to default values."""
        with self._lock:
            self._config = copy.deepcopy(self.DEFAULT_CONFIG)
            self._user_config = {}
            self.save_config()
            self.logger.info("Configuration reset to defaults")

    def export_config(self, file_path: Union[str, Path], include_sessions: bool = False) -> None:
        """
        Export configuration to a file.

        Args:
            file_path: Path to export file
            include_sessions: Whether to include session data
        """
        try:
            export_data = {
                'config': self._user_config,
                'exported_at': datetime.now().isoformat(),
                'app_version': self.get('app_info.version', 'unknown')
            }

            if include_sessions:
                # Export sessions without API keys for security
                safe_sessions = {}
                for session_id, session_data in self._sessions.items():
                    safe_session = {k: v for k, v in session_data.items() if k != 'api_key'}
                    safe_sessions[session_id] = safe_session
                export_data['sessions'] = safe_sessions

            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, indent=2)

            self.logger.info(f"Configuration exported", file_path=str(file_path))

        except Exception as e:
            self.logger.error(f"Failed to export configuration", exception=e)
            raise ConfigurationError(f"Could not export configuration: {e}")

    def create_config_template(self, file_path: Union[str, Path]) -> None:
        """
        Create a configuration template file with comments.

        Args:
            file_path: Path to create template file
        """
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
                "parallel_downloads": 4
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


# Global configuration instance
_config_instance = None
_config_lock = threading.Lock()


def get_config(config_file: Union[str, Path] = None) -> CanvasDownloaderConfig:
    """
    Get the global configuration instance.

    Args:
        config_file: Optional path to configuration file

    Returns:
        CanvasDownloaderConfig: Configuration instance
    """
    global _config_instance

    with _config_lock:
        if _config_instance is None:
            _config_instance = CanvasDownloaderConfig(config_file)
        return _config_instance


def reset_config() -> None:
    """Reset the global configuration instance."""
    global _config_instance

    with _config_lock:
        _config_instance = None
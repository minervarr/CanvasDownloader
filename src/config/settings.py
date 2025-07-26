"""
Configuration Settings Module

This module handles loading and managing application configuration settings.
It provides a centralized way to access all configuration parameters including
download settings, file naming patterns, content types, and internationalization.

The configuration is loaded from a JSON file and validated using Pydantic models
to ensure type safety and proper structure.
"""

import json
from pathlib import Path
from typing import Dict, Any, Optional
from pydantic import BaseModel, Field, validator
import os


class DownloadSettings(BaseModel):
    """
    Configuration model for download-related settings.

    Attributes:
        base_download_path: Root directory where all downloads will be stored
        parallel_downloads: Number of simultaneous downloads (1-10)
        chunk_size: Size of data chunks for file downloads (in bytes)
        retry_attempts: Number of retry attempts for failed downloads
        timeout: Request timeout in seconds
        skip_existing: Whether to skip files that already exist locally
    """
    base_download_path: str = Field(default="./downloads")
    parallel_downloads: int = Field(default=3, ge=1, le=10)
    chunk_size: int = Field(default=8192, ge=1024)
    retry_attempts: int = Field(default=3, ge=1, le=10)
    timeout: int = Field(default=30, ge=5, le=300)
    skip_existing: bool = Field(default=True)

    @validator('base_download_path')
    def validate_download_path(cls, v):
        """Ensure the download path is valid and can be created."""
        path = Path(v)
        try:
            path.mkdir(parents=True, exist_ok=True)
            return str(path.resolve())
        except Exception as e:
            raise ValueError(f"Invalid download path: {e}")


class FileNaming(BaseModel):
    """
    Configuration model for file naming conventions.

    Attributes:
        pattern: Template string for generating filenames
        sanitize_filenames: Whether to remove invalid characters from filenames
        max_filename_length: Maximum allowed filename length
    """
    pattern: str = Field(default="{type}_{number:03d}_{name}")
    sanitize_filenames: bool = Field(default=True)
    max_filename_length: int = Field(default=255, ge=50, le=255)


class ContentTypes(BaseModel):
    """
    Configuration model for which content types to download.

    Each attribute corresponds to a Canvas content type that can be enabled/disabled.
    """
    announcements: bool = Field(default=True)
    modules: bool = Field(default=True)
    assignments: bool = Field(default=True)
    quizzes: bool = Field(default=True)
    discussions: bool = Field(default=True)
    grades: bool = Field(default=True)
    people: bool = Field(default=True)
    chat: bool = Field(default=False)  # Chat might not be available in all Canvas instances


class DateFormat(BaseModel):
    """
    Configuration model for date formatting and folder structure.

    Attributes:
        locale: Locale string for date formatting (e.g., 'en_US', 'es_ES')
        format: strftime format string for dates
        folder_structure: Template for organizing course folders
    """
    locale: str = Field(default="en_US")
    format: str = Field(default="%Y-%m-%d")
    folder_structure: str = Field(default="{year}/{semester} Semester/{course_name}")


class LoggingConfig(BaseModel):
    """
    Configuration model for logging settings.

    Attributes:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        file: Path to log file
        max_size: Maximum log file size before rotation
        backup_count: Number of backup log files to keep
    """
    level: str = Field(default="INFO")
    file: str = Field(default="logs/canvas_downloader.log")
    max_size: str = Field(default="10MB")
    backup_count: int = Field(default=5, ge=1, le=20)

    @validator('level')
    def validate_log_level(cls, v):
        """Ensure log level is valid."""
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if v.upper() not in valid_levels:
            raise ValueError(f"Invalid log level. Must be one of: {valid_levels}")
        return v.upper()


class AppConfig(BaseModel):
    """
    Main configuration model that combines all configuration sections.

    This is the root configuration object that contains all other configuration
    sections. It provides methods to load, save, and validate the complete
    application configuration.
    """
    download_settings: DownloadSettings = Field(default_factory=DownloadSettings)
    file_naming: FileNaming = Field(default_factory=FileNaming)
    content_types: ContentTypes = Field(default_factory=ContentTypes)
    date_format: DateFormat = Field(default_factory=DateFormat)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)

    class Config:
        """Pydantic configuration for the AppConfig model."""
        # Allow population by field name for JSON loading
        allow_population_by_field_name = True
        # Validate assignment to catch configuration errors early
        validate_assignment = True


class ConfigManager:
    """
    Configuration Manager Class

    This class handles loading, saving, and managing the application configuration.
    It provides a singleton pattern to ensure consistent configuration access
    throughout the application.

    Usage:
        config_manager = ConfigManager()
        config = config_manager.get_config()

        # Update parallel downloads setting
        config.download_settings.parallel_downloads = 5
        config_manager.save_config()
    """

    _instance: Optional['ConfigManager'] = None
    _config: Optional[AppConfig] = None

    def __new__(cls) -> 'ConfigManager':
        """Implement singleton pattern for ConfigManager."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, config_path: str = "config.json"):
        """
        Initialize the ConfigManager.

        Args:
            config_path: Path to the configuration file
        """
        if not hasattr(self, 'initialized'):
            self.config_path = Path(config_path)
            self.initialized = True
            self._load_config()

    def _load_config(self) -> None:
        """
        Load configuration from file or create default configuration.

        This method attempts to load the configuration from the specified file.
        If the file doesn't exist or is invalid, it creates a default configuration
        and saves it to the file.
        """
        try:
            if self.config_path.exists():
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    config_data = json.load(f)
                self._config = AppConfig(**config_data)
            else:
                # Create default configuration
                self._config = AppConfig()
                self._save_config()
                print(f"Created default configuration file: {self.config_path}")
        except (json.JSONDecodeError, ValueError) as e:
            print(f"Error loading configuration: {e}")
            print("Creating default configuration...")
            self._config = AppConfig()
            self._save_config()

    def _save_config(self) -> None:
        """
        Save the current configuration to file.

        This method serializes the current configuration to JSON and writes it
        to the configuration file. It creates the directory if it doesn't exist.
        """
        try:
            # Ensure the config directory exists
            self.config_path.parent.mkdir(parents=True, exist_ok=True)

            # Convert config to dictionary and save as JSON
            config_dict = self._config.dict()
            with open(self.config_path, 'w', encoding='utf-8') as f:
                json.dump(config_dict, f, indent=4, ensure_ascii=False)
        except Exception as e:
            raise RuntimeError(f"Failed to save configuration: {e}")

    def get_config(self) -> AppConfig:
        """
        Get the current application configuration.

        Returns:
            AppConfig: The current configuration object
        """
        if self._config is None:
            self._load_config()
        return self._config

    def save_config(self) -> None:
        """
        Save the current configuration to file.

        This is a public method that allows external code to trigger
        configuration saving after making changes.
        """
        if self._config is not None:
            self._save_config()

    def update_parallel_downloads(self, count: int) -> None:
        """
        Update the parallel downloads setting.

        Args:
            count: Number of parallel downloads (1-10)

        Raises:
            ValueError: If count is outside valid range
        """
        if not 1 <= count <= 10:
            raise ValueError("Parallel downloads must be between 1 and 10")

        self._config.download_settings.parallel_downloads = count
        self._save_config()

    def get_download_path(self) -> Path:
        """
        Get the base download path as a Path object.

        Returns:
            Path: The base download directory path
        """
        return Path(self._config.download_settings.base_download_path)

    def is_content_type_enabled(self, content_type: str) -> bool:
        """
        Check if a specific content type is enabled for download.

        Args:
            content_type: Name of the content type (e.g., 'announcements', 'assignments')

        Returns:
            bool: True if the content type is enabled, False otherwise
        """
        return getattr(self._config.content_types, content_type, False)

    def get_folder_structure_template(self) -> str:
        """
        Get the folder structure template string.

        Returns:
            str: Template string for organizing course folders
        """
        return self._config.date_format.folder_structure


# Global configuration manager instance
config_manager = ConfigManager()

def get_config() -> AppConfig:
    """
    Convenience function to get the application configuration.

    Returns:
        AppConfig: The current application configuration
    """
    return config_manager.get_config()
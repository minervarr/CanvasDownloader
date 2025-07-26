"""
Sessions Management Module

This module handles Canvas API sessions, providing a high-level interface for
managing multiple Canvas accounts/institutions. It integrates with the encryption
module to securely store and retrieve API credentials.

Features:
- Create, list, select, and delete Canvas sessions
- Validate API credentials before saving
- Session metadata management (institution name, last used, etc.)
- Integration with Canvas API client
- User-friendly session selection interface

Usage:
    # Initialize session manager
    session_manager = SessionManager()

    # Add a new session
    session_manager.add_session(
        session_name="My University",
        api_url="https://canvas.university.edu",
        api_key="your_api_key_here",
        institution_name="University Name"
    )

    # List available sessions
    sessions = session_manager.list_sessions()

    # Select and use a session
    credentials = session_manager.select_session("My University")
"""

import re
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from urllib.parse import urlparse
import requests

from .encryption import EncryptionManager, EncryptionError


class SessionValidationError(Exception):
    """Custom exception for session validation errors."""
    pass


class SessionManager:
    """
    Canvas Session Manager

    This class provides a high-level interface for managing Canvas API sessions.
    It handles session creation, validation, storage, and retrieval while
    integrating with the encryption system for secure credential storage.

    The session manager ensures that:
    - API URLs are valid Canvas URLs
    - API keys are properly formatted
    - Credentials are validated before saving
    - Session names are unique and valid
    - Metadata is properly maintained
    """

    def __init__(self, encryption_manager: EncryptionManager = None):
        """
        Initialize the SessionManager.

        Args:
            encryption_manager: Optional EncryptionManager instance.
                               If None, uses the global instance.
        """
        if encryption_manager is None:
            from .encryption import get_encryption_manager
            self.encryption_manager = get_encryption_manager()
        else:
            self.encryption_manager = encryption_manager

    def validate_api_url(self, api_url: str) -> str:
        """
        Validate and normalize a Canvas API URL.

        This method checks that the provided URL is a valid Canvas instance URL
        and normalizes it to a standard format.

        Args:
            api_url: The Canvas API URL to validate

        Returns:
            str: The normalized and validated API URL

        Raises:
            SessionValidationError: If the URL is invalid
        """
        try:
            # Remove trailing slashes and whitespace
            api_url = api_url.strip().rstrip('/')

            # Add https:// if no protocol specified
            if not api_url.startswith(('http://', 'https://')):
                api_url = 'https://' + api_url

            # Parse the URL to validate structure
            parsed = urlparse(api_url)

            # Basic URL validation
            if not parsed.netloc:
                raise ValueError("Invalid URL: No domain found")

            if not parsed.scheme in ('http', 'https'):
                raise ValueError("Invalid URL: Must use http or https")

            # Check for common Canvas URL patterns
            domain = parsed.netloc.lower()
            canvas_indicators = [
                'canvas.',
                '.instructure.com',
                'lms.',
                'elearning.',
                'blackboard.'  # Some institutions use Blackboard branding
            ]

            # Warn if URL doesn't look like Canvas (but don't fail)
            if not any(indicator in domain for indicator in canvas_indicators):
                print(f"Warning: URL '{api_url}' doesn't appear to be a Canvas instance")

            return api_url

        except Exception as e:
            raise SessionValidationError(f"Invalid API URL: {e}")

    def validate_api_key(self, api_key: str) -> str:
        """
        Validate a Canvas API key format.

        Canvas API keys have specific format requirements. This method
        validates the key format without actually testing it against the API.

        Args:
            api_key: The Canvas API key to validate

        Returns:
            str: The validated API key

        Raises:
            SessionValidationError: If the API key format is invalid
        """
        try:
            # Remove whitespace
            api_key = api_key.strip()

            # Check for empty key
            if not api_key:
                raise ValueError("API key cannot be empty")

            # Canvas API keys are typically long alphanumeric strings
            # They usually contain numbers, letters, and sometimes special characters
            if len(api_key) < 10:
                raise ValueError("API key appears too short (minimum 10 characters)")

            if len(api_key) > 200:
                raise ValueError("API key appears too long (maximum 200 characters)")

            # Check for obviously invalid characters (spaces, quotes, etc.)
            invalid_chars = ['"', "'", ' ', '\t', '\n', '\r']
            for char in invalid_chars:
                if char in api_key:
                    raise ValueError(f"API key contains invalid character: '{char}'")

            return api_key

        except Exception as e:
            raise SessionValidationError(f"Invalid API key: {e}")

    def validate_session_name(self, session_name: str, exclude_existing: str = None) -> str:
        """
        Validate a session name for uniqueness and format.

        Args:
            session_name: The proposed session name
            exclude_existing: Optional existing session name to exclude from uniqueness check
                             (useful when updating an existing session)

        Returns:
            str: The validated session name

        Raises:
            SessionValidationError: If the session name is invalid or already exists
        """
        try:
            # Clean up the session name
            session_name = session_name.strip()

            # Check for empty name
            if not session_name:
                raise ValueError("Session name cannot be empty")

            # Check length
            if len(session_name) > 100:
                raise ValueError("Session name too long (maximum 100 characters)")

            # Check for invalid characters (those that might cause file system issues)
            invalid_chars = ['/', '\\', ':', '*', '?', '"', '<', '>', '|']
            for char in invalid_chars:
                if char in session_name:
                    raise ValueError(f"Session name contains invalid character: '{char}'")

            # Check for uniqueness
            existing_sessions = self.list_sessions()
            for session in existing_sessions:
                if (session['session_name'] == session_name and
                        session_name != exclude_existing):
                    raise ValueError(f"Session name '{session_name}' already exists")

            return session_name

        except Exception as e:
            raise SessionValidationError(f"Invalid session name: {e}")

    def test_api_connection(self, api_url: str, api_key: str) -> Tuple[bool, str]:
        """
        Test Canvas API connection with provided credentials.

        This method makes a simple API call to verify that the credentials
        are valid and the Canvas instance is accessible.

        Args:
            api_url: Canvas API URL
            api_key: Canvas API key

        Returns:
            Tuple[bool, str]: (success, message)
                             success: True if connection successful
                             message: Success message or error description
        """
        try:
            # Construct API endpoint for user profile
            test_url = f"{api_url}/api/v1/users/self/profile"

            # Prepare headers
            headers = {
                'Authorization': f'Bearer {api_key}',
                'Content-Type': 'application/json',
                'User-Agent': 'Canvas-Downloader/1.0'
            }

            # Make test request with short timeout
            response = requests.get(test_url, headers=headers, timeout=10)

            if response.status_code == 200:
                # Parse response to get user info
                user_data = response.json()
                username = user_data.get('name', 'Unknown User')
                return True, f"Connection successful! Logged in as: {username}"

            elif response.status_code == 401:
                return False, "Authentication failed. Please check your API key."

            elif response.status_code == 403:
                return False, "Access forbidden. Your API key may not have sufficient permissions."

            elif response.status_code == 404:
                return False, "Canvas instance not found. Please check your API URL."

            else:
                return False, f"API request failed with status code: {response.status_code}"

        except requests.exceptions.Timeout:
            return False, "Connection timeout. Please check your internet connection and API URL."

        except requests.exceptions.ConnectionError:
            return False, "Connection error. Please check your API URL and internet connection."

        except requests.exceptions.RequestException as e:
            return False, f"Request error: {e}"

        except Exception as e:
            return False, f"Unexpected error: {e}"

    def add_session(self, session_name: str, api_url: str, api_key: str,
                    institution_name: str = "", validate_connection: bool = True) -> bool:
        """
        Add a new Canvas session with encrypted credentials.

        This method validates the provided information, optionally tests the
        connection, and saves the session with encrypted credentials.

        Args:
            session_name: Unique name for this session
            api_url: Canvas API URL
            api_key: Canvas API key
            institution_name: Optional institution name
            validate_connection: Whether to test the API connection before saving

        Returns:
            bool: True if session was added successfully

        Raises:
            SessionValidationError: If validation fails
        """
        try:
            # Validate all inputs
            session_name = self.validate_session_name(session_name)
            api_url = self.validate_api_url(api_url)
            api_key = self.validate_api_key(api_key)

            # Test connection if requested
            if validate_connection:
                success, message = self.test_api_connection(api_url, api_key)
                if not success:
                    raise SessionValidationError(f"Connection test failed: {message}")
                print(f"✓ {message}")

            # Prepare credentials dictionary
            credentials = {
                'api_url': api_url,
                'api_key': api_key
            }

            # Save encrypted session
            self.encryption_manager.save_session(
                session_name=session_name,
                credentials=credentials,
                institution_name=institution_name
            )

            print(f"✓ Session '{session_name}' added successfully!")
            return True

        except (SessionValidationError, EncryptionError) as e:
            print(f"✗ Failed to add session: {e}")
            return False

        except Exception as e:
            print(f"✗ Unexpected error adding session: {e}")
            return False

    def list_sessions(self) -> List[Dict[str, str]]:
        """
        List all available Canvas sessions.

        Returns:
            List[Dict[str, str]]: List of session information dictionaries
        """
        try:
            return self.encryption_manager.list_sessions()
        except EncryptionError as e:
            print(f"Error listing sessions: {e}")
            return []

    def select_session(self, session_name: str) -> Optional[Dict[str, str]]:
        """
        Select and load a Canvas session.

        This method loads the specified session and returns the decrypted
        credentials for use with the Canvas API.

        Args:
            session_name: Name of the session to load

        Returns:
            Optional[Dict[str, str]]: Decrypted credentials or None if failed
        """
        try:
            credentials = self.encryption_manager.load_session(session_name)
            print(f"✓ Loaded session: {session_name}")
            return credentials
        except EncryptionError as e:
            print(f"✗ Failed to load session '{session_name}': {e}")
            return None

    def delete_session(self, session_name: str) -> bool:
        """
        Delete a Canvas session.

        Args:
            session_name: Name of the session to delete

        Returns:
            bool: True if session was deleted successfully
        """
        try:
            if self.encryption_manager.delete_session(session_name):
                print(f"✓ Session '{session_name}' deleted successfully!")
                return True
            else:
                print(f"✗ Session '{session_name}' not found")
                return False
        except EncryptionError as e:
            print(f"✗ Failed to delete session: {e}")
            return False

    def update_session(self, session_name: str, new_session_name: str = None,
                       api_url: str = None, api_key: str = None,
                       institution_name: str = None, validate_connection: bool = True) -> bool:
        """
        Update an existing Canvas session.

        This method allows updating any aspect of an existing session while
        maintaining the same encryption and validation standards.

        Args:
            session_name: Current session name
            new_session_name: New session name (optional)
            api_url: New API URL (optional)
            api_key: New API key (optional)
            institution_name: New institution name (optional)
            validate_connection: Whether to test the API connection

        Returns:
            bool: True if session was updated successfully
        """
        try:
            # Load existing session
            existing_credentials = self.encryption_manager.load_session(session_name)
            if not existing_credentials:
                raise SessionValidationError(f"Session '{session_name}' not found")

            # Get existing session metadata
            sessions = self.encryption_manager.list_sessions()
            existing_session = None
            for session in sessions:
                if session['session_name'] == session_name:
                    existing_session = session
                    break

            if not existing_session:
                raise SessionValidationError(f"Session metadata for '{session_name}' not found")

            # Use existing values if new ones not provided
            updated_name = new_session_name if new_session_name else session_name
            updated_url = api_url if api_url else existing_credentials['api_url']
            updated_key = api_key if api_key else existing_credentials['api_key']
            updated_institution = (institution_name if institution_name is not None
                                   else existing_session['institution_name'])

            # Validate updates
            if new_session_name:
                updated_name = self.validate_session_name(updated_name, exclude_existing=session_name)
            if api_url:
                updated_url = self.validate_api_url(updated_url)
            if api_key:
                updated_key = self.validate_api_key(updated_key)

            # Test connection if credentials changed
            if (api_url or api_key) and validate_connection:
                success, message = self.test_api_connection(updated_url, updated_key)
                if not success:
                    raise SessionValidationError(f"Connection test failed: {message}")
                print(f"✓ {message}")

            # Delete old session if name changed
            if updated_name != session_name:
                self.encryption_manager.delete_session(session_name)

            # Save updated session
            updated_credentials = {
                'api_url': updated_url,
                'api_key': updated_key
            }

            self.encryption_manager.save_session(
                session_name=updated_name,
                credentials=updated_credentials,
                institution_name=updated_institution
            )

            print(f"✓ Session updated successfully!")
            return True

        except (SessionValidationError, EncryptionError) as e:
            print(f"✗ Failed to update session: {e}")
            return False

        except Exception as e:
            print(f"✗ Unexpected error updating session: {e}")
            return False

    def get_session_info(self, session_name: str) -> Optional[Dict[str, str]]:
        """
        Get information about a specific session without decrypting credentials.

        Args:
            session_name: Name of the session

        Returns:
            Optional[Dict[str, str]]: Session information or None if not found
        """
        sessions = self.list_sessions()
        for session in sessions:
            if session['session_name'] == session_name:
                return session
        return None

    def export_session_backup(self, session_name: str, include_credentials: bool = False) -> Optional[Dict]:
        """
        Export session data for backup purposes.

        Args:
            session_name: Name of the session to export
            include_credentials: Whether to include decrypted credentials (WARNING: Not secure!)

        Returns:
            Optional[Dict]: Session backup data or None if failed
        """
        try:
            session_info = self.get_session_info(session_name)
            if not session_info:
                return None

            backup_data = session_info.copy()

            if include_credentials:
                credentials = self.encryption_manager.load_session(session_name)
                backup_data['credentials'] = credentials
                print("WARNING: Exported credentials are NOT encrypted!")

            return backup_data

        except Exception as e:
            print(f"Error exporting session backup: {e}")
            return None


# Global session manager instance
session_manager = SessionManager()


def get_session_manager() -> SessionManager:
    """
    Get the global session manager instance.

    Returns:
        SessionManager: The global session manager
    """
    return session_manager
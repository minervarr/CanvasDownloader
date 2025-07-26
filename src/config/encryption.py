"""
Encryption Module for API Credentials

This module provides secure encryption and decryption functionality for storing
Canvas API credentials. It uses Fernet symmetric encryption from the cryptography
library to ensure that sensitive information like API keys and URLs are stored
securely on disk.

The module handles:
- Generating and managing encryption keys
- Encrypting/decrypting API credentials
- Secure storage of encrypted session data
- Key derivation from user passwords (optional)

Usage:
    # Initialize encryption manager
    encryptor = EncryptionManager()

    # Encrypt credentials
    encrypted_data = encryptor.encrypt_credentials({
        'api_url': 'https://canvas.example.edu',
        'api_key': 'secret_api_key_here'
    })

    # Decrypt credentials
    credentials = encryptor.decrypt_credentials(encrypted_data)
"""

import os
import json
import base64
from pathlib import Path
from typing import Dict, Any, Optional, List
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import secrets
import getpass


class EncryptionError(Exception):
    """Custom exception for encryption-related errors."""
    pass


class SessionData:
    """
    Data class for storing encrypted session information.

    This class represents a Canvas session with encrypted credentials
    and associated metadata.

    Attributes:
        session_name: Human-readable name for the session
        encrypted_data: Encrypted credentials data
        institution_name: Name of the educational institution
        created_at: Timestamp when session was created
        last_used: Timestamp when session was last accessed
    """

    def __init__(self, session_name: str, encrypted_data: bytes,
                 institution_name: str = "", created_at: str = "",
                 last_used: str = ""):
        self.session_name = session_name
        self.encrypted_data = encrypted_data
        self.institution_name = institution_name
        self.created_at = created_at
        self.last_used = last_used

    def to_dict(self) -> Dict[str, Any]:
        """Convert session data to dictionary for JSON serialization."""
        return {
            'session_name': self.session_name,
            'encrypted_data': base64.b64encode(self.encrypted_data).decode('utf-8'),
            'institution_name': self.institution_name,
            'created_at': self.created_at,
            'last_used': self.last_used
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SessionData':
        """Create SessionData instance from dictionary."""
        encrypted_data = base64.b64decode(data['encrypted_data'].encode('utf-8'))
        return cls(
            session_name=data['session_name'],
            encrypted_data=encrypted_data,
            institution_name=data.get('institution_name', ''),
            created_at=data.get('created_at', ''),
            last_used=data.get('last_used', '')
        )


class EncryptionManager:
    """
    Encryption Manager for Canvas API Credentials

    This class handles all encryption and decryption operations for Canvas
    API credentials. It provides methods to:
    - Generate and manage encryption keys
    - Encrypt and decrypt credential data
    - Store and retrieve multiple sessions
    - Derive keys from passwords for additional security

    The encryption uses Fernet (AES 128 in CBC mode with HMAC-SHA256 for authentication)
    which provides authenticated symmetric encryption.
    """

    def __init__(self, sessions_file: str = "sessions.enc"):
        """
        Initialize the EncryptionManager.

        Args:
            sessions_file: Path to the file where encrypted sessions are stored
        """
        self.sessions_file = Path(sessions_file)
        self.sessions_dir = self.sessions_file.parent
        self.key_file = self.sessions_dir / "master.key"

        # Ensure the sessions directory exists
        self.sessions_dir.mkdir(parents=True, exist_ok=True)

        # Initialize or load the master encryption key
        self._master_key = self._load_or_create_master_key()
        self._cipher = Fernet(self._master_key)

    def _generate_key(self) -> bytes:
        """
        Generate a new Fernet encryption key.

        Returns:
            bytes: A new 32-byte encryption key suitable for Fernet
        """
        return Fernet.generate_key()

    def _derive_key_from_password(self, password: str, salt: bytes = None) -> tuple[bytes, bytes]:
        """
        Derive an encryption key from a password using PBKDF2.

        This method uses PBKDF2 (Password-Based Key Derivation Function 2) to derive
        a cryptographically strong key from a user password. This adds an extra layer
        of security by requiring the user to enter a password to decrypt sessions.

        Args:
            password: The password to derive the key from
            salt: Optional salt bytes. If None, a new salt is generated

        Returns:
            tuple: (derived_key, salt_used)
        """
        if salt is None:
            salt = os.urandom(16)  # Generate 16-byte salt

        # Configure PBKDF2 with SHA256 and 100,000 iterations
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,  # Fernet requires 32-byte keys
            salt=salt,
            iterations=100000,  # NIST recommended minimum
        )

        # Derive key from password
        key = base64.urlsafe_b64encode(kdf.derive(password.encode('utf-8')))
        return key, salt

    def _load_or_create_master_key(self) -> bytes:
        """
        Load existing master key or create a new one.

        The master key is used to encrypt individual session data. It's stored
        in a separate file and loaded on initialization.

        Returns:
            bytes: The master encryption key
        """
        try:
            if self.key_file.exists():
                # Load existing key
                with open(self.key_file, 'rb') as f:
                    return f.read()
            else:
                # Generate new master key
                master_key = self._generate_key()

                # Save the key securely
                with open(self.key_file, 'wb') as f:
                    f.write(master_key)

                # Set restrictive permissions (owner read/write only)
                os.chmod(self.key_file, 0o600)

                return master_key

        except Exception as e:
            raise EncryptionError(f"Failed to load or create master key: {e}")

    def encrypt_credentials(self, credentials: Dict[str, str]) -> bytes:
        """
        Encrypt Canvas API credentials.

        Takes a dictionary containing Canvas API credentials and encrypts them
        using Fernet symmetric encryption.

        Args:
            credentials: Dictionary containing 'api_url' and 'api_key'

        Returns:
            bytes: Encrypted credentials data

        Raises:
            EncryptionError: If encryption fails
        """
        try:
            # Validate required fields
            required_fields = ['api_url', 'api_key']
            for field in required_fields:
                if field not in credentials:
                    raise ValueError(f"Missing required field: {field}")

            # Convert credentials to JSON and encrypt
            credentials_json = json.dumps(credentials)
            encrypted_data = self._cipher.encrypt(credentials_json.encode('utf-8'))

            return encrypted_data

        except Exception as e:
            raise EncryptionError(f"Failed to encrypt credentials: {e}")

    def decrypt_credentials(self, encrypted_data: bytes) -> Dict[str, str]:
        """
        Decrypt Canvas API credentials.

        Takes encrypted credentials data and returns the original dictionary
        containing the API URL and key.

        Args:
            encrypted_data: Encrypted credentials bytes

        Returns:
            Dict[str, str]: Decrypted credentials containing 'api_url' and 'api_key'

        Raises:
            EncryptionError: If decryption fails
        """
        try:
            # Decrypt the data
            decrypted_bytes = self._cipher.decrypt(encrypted_data)
            credentials_json = decrypted_bytes.decode('utf-8')

            # Parse JSON and return credentials
            credentials = json.loads(credentials_json)
            return credentials

        except Exception as e:
            raise EncryptionError(f"Failed to decrypt credentials: {e}")

    def save_session(self, session_name: str, credentials: Dict[str, str],
                     institution_name: str = "") -> None:
        """
        Save a new Canvas session with encrypted credentials.

        This method encrypts the provided credentials and saves them as a named
        session that can be retrieved later.

        Args:
            session_name: Unique name for this session
            credentials: Dictionary containing 'api_url' and 'api_key'
            institution_name: Optional name of the institution

        Raises:
            EncryptionError: If saving fails
        """
        try:
            from datetime import datetime

            # Encrypt the credentials
            encrypted_data = self.encrypt_credentials(credentials)

            # Create session data
            session = SessionData(
                session_name=session_name,
                encrypted_data=encrypted_data,
                institution_name=institution_name,
                created_at=datetime.now().isoformat(),
                last_used=datetime.now().isoformat()
            )

            # Load existing sessions
            sessions = self._load_sessions()

            # Add or update the session
            sessions[session_name] = session

            # Save all sessions
            self._save_sessions(sessions)

        except Exception as e:
            raise EncryptionError(f"Failed to save session: {e}")

    def load_session(self, session_name: str) -> Dict[str, str]:
        """
        Load and decrypt a saved Canvas session.

        Args:
            session_name: Name of the session to load

        Returns:
            Dict[str, str]: Decrypted credentials

        Raises:
            EncryptionError: If session not found or decryption fails
        """
        try:
            sessions = self._load_sessions()

            if session_name not in sessions:
                raise ValueError(f"Session '{session_name}' not found")

            session = sessions[session_name]

            # Update last used timestamp
            from datetime import datetime
            session.last_used = datetime.now().isoformat()
            sessions[session_name] = session
            self._save_sessions(sessions)

            # Decrypt and return credentials
            return self.decrypt_credentials(session.encrypted_data)

        except Exception as e:
            raise EncryptionError(f"Failed to load session: {e}")

    def list_sessions(self) -> List[Dict[str, str]]:
        """
        List all saved Canvas sessions.

        Returns:
            List[Dict[str, str]]: List of session information (without credentials)
        """
        try:
            sessions = self._load_sessions()

            session_list = []
            for name, session in sessions.items():
                session_info = {
                    'session_name': session.session_name,
                    'institution_name': session.institution_name,
                    'created_at': session.created_at,
                    'last_used': session.last_used
                }
                session_list.append(session_info)

            return session_list

        except Exception as e:
            raise EncryptionError(f"Failed to list sessions: {e}")

    def delete_session(self, session_name: str) -> bool:
        """
        Delete a saved Canvas session.

        Args:
            session_name: Name of the session to delete

        Returns:
            bool: True if session was deleted, False if not found
        """
        try:
            sessions = self._load_sessions()

            if session_name in sessions:
                del sessions[session_name]
                self._save_sessions(sessions)
                return True

            return False

        except Exception as e:
            raise EncryptionError(f"Failed to delete session: {e}")

    def _load_sessions(self) -> Dict[str, SessionData]:
        """
        Load all sessions from the encrypted sessions file.

        Returns:
            Dict[str, SessionData]: Dictionary of session name to SessionData
        """
        if not self.sessions_file.exists():
            return {}

        try:
            with open(self.sessions_file, 'r', encoding='utf-8') as f:
                sessions_data = json.load(f)

            sessions = {}
            for name, data in sessions_data.items():
                sessions[name] = SessionData.from_dict(data)

            return sessions

        except Exception as e:
            # If file is corrupted, start fresh
            print(f"Warning: Could not load sessions file: {e}")
            return {}

    def _save_sessions(self, sessions: Dict[str, SessionData]) -> None:
        """
        Save all sessions to the encrypted sessions file.

        Args:
            sessions: Dictionary of session name to SessionData
        """
        sessions_data = {}
        for name, session in sessions.items():
            sessions_data[name] = session.to_dict()

        with open(self.sessions_file, 'w', encoding='utf-8') as f:
            json.dump(sessions_data, f, indent=2, ensure_ascii=False)

        # Set restrictive permissions
        os.chmod(self.sessions_file, 0o600)

    def change_master_password(self, old_password: str = None, new_password: str = None) -> None:
        """
        Change the master password used for key derivation.

        This is an advanced feature that allows using password-based encryption
        instead of just file-based key storage.

        Args:
            old_password: Current password (None if not using passwords)
            new_password: New password to set
        """
        # This would require re-encrypting all sessions with a new key
        # Implementation would depend on whether password-based encryption is used
        raise NotImplementedError("Password-based encryption not yet implemented")


# Global encryption manager instance
encryption_manager = EncryptionManager()


def get_encryption_manager() -> EncryptionManager:
    """
    Get the global encryption manager instance.

    Returns:
        EncryptionManager: The global encryption manager
    """
    return encryption_manager
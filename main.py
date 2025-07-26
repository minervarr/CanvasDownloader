#!/usr/bin/env python3
"""
Canvas Downloader - Main Entry Point

This application downloads course content from Canvas LMS including:
- Announcements
- Modules
- Assignments
- Quizzes
- Discussions
- Grades
- People
- Chat

The application supports:
- Multiple Canvas sessions with encrypted credentials
- Parallel downloads with progress tracking
- Customizable folder structure
- Internationalization support
- Skip existing files to avoid re-downloading
"""

import sys
from src.ui.cli import main

if __name__ == "__main__":
    sys.exit(main())

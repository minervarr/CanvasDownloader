# Canvas Downloader

A modular Python application for downloading course content from Canvas LMS.

## Features

- **Comprehensive Content Download**: Downloads Announcements, Modules, Assignments, Quizzes, Discussions, Grades, People, and Chat
- **Smart Organization**: Automatically organizes courses by year/semester/course structure
- **Parallel Downloads**: Configurable parallel download support with progress tracking
- **Secure Credential Storage**: Encrypted API credentials using Fernet encryption
- **Multiple Sessions**: Support for multiple Canvas accounts/institutions
- **Skip Existing**: Intelligent duplicate detection to avoid re-downloading
- **Internationalization Ready**: Built with Babel support for multiple languages
- **Modular Architecture**: Easy to extend and maintain

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/canvas-downloader.git
cd canvas-downloader

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Install in development mode
pip install -e .
```

## Quick Start

1. Run the application:
   ```bash
   python main.py
   ```

2. On first run, you'll be prompted to:
   - Add a new Canvas session
   - Enter your Canvas URL (e.g., https://canvas.yourschool.edu)
   - Enter your API key (generated from Canvas settings)

3. Select courses to download and content types

4. Watch the progress as your content downloads!

## Configuration

See `config.json.example` for all available configuration options.

## Project Structure

```
canvas-downloader/
├── src/
│   ├── api/          # Canvas API client and authentication
│   ├── config/       # Configuration and encryption management
│   ├── core/         # Core application logic
│   ├── downloaders/  # Content-specific download modules
│   ├── models/       # Data models
│   ├── utils/        # Utility functions
│   └── ui/           # User interface
├── tests/            # Unit and integration tests
├── downloads/        # Default download directory
├── logs/             # Application logs
└── locales/          # Internationalization files
```

## Contributing

Please read CONTRIBUTING.md for details on our code of conduct and the process for submitting pull requests.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

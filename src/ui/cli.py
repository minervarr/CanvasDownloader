"""
Command Line Interface Module

This module provides the main command-line interface for the Canvas Downloader
application. It offers an interactive menu system for managing Canvas sessions,
selecting courses, configuring download options, and monitoring progress.

The CLI provides:
- Interactive menu navigation
- Canvas session management (add, select, delete sessions)
- Course selection and filtering
- Content type configuration
- Download progress monitoring
- Error handling and user feedback
- Configuration management
- Help and documentation

Features:
- Rich console output with colors and formatting
- Intuitive menu navigation with keyboard shortcuts
- Real-time progress display during downloads
- Comprehensive error messages and troubleshooting
- Session persistence and management
- Flexible course filtering and selection
- Download resumption and status checking

Usage:
    python main.py

    # Or directly:
    python -m src.ui.cli

The CLI guides users through:
1. Session setup and Canvas authentication
2. Course discovery and selection
3. Content type configuration
4. Download execution and monitoring
5. Results review and management
"""

import asyncio
import sys
import signal
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path

try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskID
    from rich.prompt import Prompt, Confirm, IntPrompt
    from rich.markdown import Markdown
    from rich.text import Text
    from rich.columns import Columns
    from rich import print as rprint

    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False
    print("Rich library not available. Using basic console output.")

from ..core.orchestrator import CanvasOrchestrator, create_orchestrator, DownloadResults
from ..config.sessions import SessionManager, get_session_manager
from ..config.settings import get_config
from ..utils.logger import get_logger, setup_logging
from ..utils.progress import ProgressTracker, create_progress_tracker


class CanvasDownloaderCLI:
    """
    Canvas Downloader Command Line Interface

    This class provides an interactive command-line interface for the Canvas
    Downloader application. It manages user interaction, session handling,
    course selection, and download coordination through an intuitive menu system.

    The CLI ensures a smooth user experience with:
    - Clear navigation and menu structure
    - Helpful prompts and validation
    - Rich visual feedback and progress tracking
    - Comprehensive error handling and recovery
    - Persistent session and configuration management
    """

    def __init__(self):
        """Initialize the Canvas Downloader CLI."""
        # Set up logging
        setup_logging()
        self.logger = get_logger(__name__)

        # Initialize components
        self.config = get_config()
        self.session_manager = get_session_manager()
        self.orchestrator = create_orchestrator()

        # Console setup
        if RICH_AVAILABLE:
            self.console = Console()
            self.use_rich = True
        else:
            self.console = None
            self.use_rich = False

        # State tracking
        self.current_session = None
        self.available_courses = []
        self.selected_courses = []
        self.enabled_content_types = set()

        # Download state
        self.is_downloading = False
        self.download_task = None

        # Initialize default content types
        self._initialize_content_types()

        self.logger.info("Canvas Downloader CLI initialized")

    def _initialize_content_types(self):
        """Initialize default content types from configuration."""
        content_config = self.config.content_types

        if content_config.announcements:
            self.enabled_content_types.add('announcements')
        if content_config.assignments:
            self.enabled_content_types.add('assignments')
        if content_config.discussions:
            self.enabled_content_types.add('discussions')
        if content_config.files:
            self.enabled_content_types.add('files')
        if content_config.modules:
            self.enabled_content_types.add('modules')
        if content_config.quizzes:
            self.enabled_content_types.add('quizzes')
        if content_config.grades:
            self.enabled_content_types.add('grades')
        if content_config.people:
            self.enabled_content_types.add('people')
        if content_config.chat:
            self.enabled_content_types.add('chat')

    def run(self) -> int:
        """
        Run the main CLI application.

        Returns:
            int: Exit code (0 for success, non-zero for error)
        """
        try:
            # Set up signal handlers for graceful shutdown
            signal.signal(signal.SIGINT, self._signal_handler)
            signal.signal(signal.SIGTERM, self._signal_handler)

            # Show welcome message
            self._show_welcome()

            # Main application loop
            asyncio.run(self._main_loop())

            return 0

        except KeyboardInterrupt:
            self._print("\n👋 Goodbye!")
            return 0
        except Exception as e:
            self.logger.error(f"CLI application error", exception=e)
            self._print_error(f"Application error: {e}")
            return 1
        finally:
            # Cleanup
            asyncio.run(self._cleanup())

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        self._print("\n🛑 Shutdown signal received...")

        if self.is_downloading:
            self._print("⏳ Stopping download...")
            self.orchestrator.stop_download()

        sys.exit(0)

    async def _main_loop(self):
        """Main application loop with menu navigation."""
        while True:
            try:
                # Show main menu
                choice = await self._show_main_menu()

                if choice == '1':
                    await self._manage_sessions_menu()
                elif choice == '2':
                    await self._select_courses_menu()
                elif choice == '3':
                    await self._configure_content_types_menu()
                elif choice == '4':
                    await self._configure_settings_menu()
                elif choice == '5':
                    await self._start_download()
                elif choice == '6':
                    await self._view_download_status()
                elif choice == '7':
                    await self._view_download_history()
                elif choice == '8':
                    self._show_help()
                elif choice == '9' or choice.lower() == 'q':
                    break
                else:
                    self._print_error("Invalid option. Please try again.")

                if choice != '9' and choice.lower() != 'q':
                    self._wait_for_enter()

            except KeyboardInterrupt:
                self._print("\n👋 Goodbye!")
                break
            except Exception as e:
                self.logger.error(f"Main loop error", exception=e)
                self._print_error(f"An error occurred: {e}")
                self._wait_for_enter()

    async def _show_main_menu(self) -> str:
        """Display the main menu and get user choice."""
        self._clear_screen()

        # Show current status
        status_info = []

        if self.current_session:
            status_info.append(f"📡 Session: {self.current_session}")
        else:
            status_info.append("📡 Session: Not connected")

        if self.selected_courses:
            status_info.append(f"📚 Courses: {len(self.selected_courses)} selected")
        else:
            status_info.append("📚 Courses: None selected")

        if self.enabled_content_types:
            status_info.append(f"📋 Content: {len(self.enabled_content_types)} types enabled")
        else:
            status_info.append("📋 Content: None enabled")

        status_text = " | ".join(status_info)

        if self.use_rich:
            # Create status panel
            status_panel = Panel(status_text, title="Current Status", border_style="blue")
            self.console.print(status_panel)
            self.console.print()

            # Create menu table
            table = Table(title="Canvas Downloader - Main Menu", show_header=False, box=None)
            table.add_column("Option", style="cyan", no_wrap=True)
            table.add_column("Description", style="white")

            table.add_row("1", "🔐 Manage Canvas Sessions")
            table.add_row("2", "📚 Select Courses")
            table.add_row("3", "📋 Configure Content Types")
            table.add_row("4", "⚙️  Configure Settings")
            table.add_row("5", "⬇️  Start Download")
            table.add_row("6", "📊 View Download Status")
            table.add_row("7", "📈 View Download History")
            table.add_row("8", "❓ Help")
            table.add_row("9", "👋 Exit")

            self.console.print(table)
            self.console.print()

            choice = Prompt.ask("Select an option", choices=["1", "2", "3", "4", "5", "6", "7", "8", "9", "q"])
        else:
            # Basic text menu
            print("=" * 60)
            print("Canvas Downloader - Main Menu")
            print("=" * 60)
            print(f"Status: {status_text}")
            print()
            print("1. Manage Canvas Sessions")
            print("2. Select Courses")
            print("3. Configure Content Types")
            print("4. Configure Settings")
            print("5. Start Download")
            print("6. View Download Status")
            print("7. View Download History")
            print("8. Help")
            print("9. Exit")
            print()
            choice = input("Select an option (1-9): ").strip()

        return choice

    async def _manage_sessions_menu(self):
        """Handle session management menu."""
        while True:
            self._clear_screen()

            # Get available sessions
            sessions = self.session_manager.list_sessions()

            if self.use_rich:
                # Show sessions table
                if sessions:
                    table = Table(title="Available Canvas Sessions")
                    table.add_column("Session Name", style="cyan")
                    table.add_column("Institution", style="green")
                    table.add_column("Last Used", style="yellow")
                    table.add_column("Status", style="magenta")

                    for session in sessions:
                        status = "🟢 Active" if session['session_name'] == self.current_session else "⚪ Available"
                        table.add_row(
                            session['session_name'],
                            session['institution_name'] or "Unknown",
                            session['last_used'][:10] if session['last_used'] else "Never",
                            status
                        )

                    self.console.print(table)
                    self.console.print()
                else:
                    self.console.print(Panel("No sessions found. Create a new session to get started.",
                                             title="Sessions", border_style="yellow"))
                    self.console.print()

                # Session menu
                menu_table = Table(show_header=False, box=None)
                menu_table.add_column("Option", style="cyan", no_wrap=True)
                menu_table.add_column("Description", style="white")

                menu_table.add_row("1", "➕ Add New Session")
                if sessions:
                    menu_table.add_row("2", "🔗 Connect to Session")
                    menu_table.add_row("3", "✏️  Edit Session")
                    menu_table.add_row("4", "🗑️  Delete Session")
                menu_table.add_row("5", "🔍 Test Connection")
                menu_table.add_row("0", "⬅️  Back to Main Menu")

                self.console.print(menu_table)
                self.console.print()

                valid_choices = ["1", "0"]
                if sessions:
                    valid_choices.extend(["2", "3", "4", "5"])

                choice = Prompt.ask("Select an option", choices=valid_choices)
            else:
                # Basic text interface
                print("Session Management")
                print("-" * 30)

                if sessions:
                    print("Available Sessions:")
                    for i, session in enumerate(sessions, 1):
                        status = "(Active)" if session['session_name'] == self.current_session else ""
                        print(f"{i}. {session['session_name']} {status}")
                    print()
                else:
                    print("No sessions found.")
                    print()

                print("1. Add New Session")
                if sessions:
                    print("2. Connect to Session")
                    print("3. Edit Session")
                    print("4. Delete Session")
                print("5. Test Connection")
                print("0. Back to Main Menu")
                print()
                choice = input("Select an option: ").strip()

            if choice == '1':
                await self._add_new_session()
            elif choice == '2' and sessions:
                await self._connect_to_session()
            elif choice == '3' and sessions:
                await self._edit_session()
            elif choice == '4' and sessions:
                await self._delete_session()
            elif choice == '5':
                await self._test_connection()
            elif choice == '0':
                break
            else:
                self._print_error("Invalid option.")

            if choice != '0':
                self._wait_for_enter()

    async def _add_new_session(self):
        """Add a new Canvas session."""
        self._print_header("Add New Canvas Session")

        try:
            if self.use_rich:
                session_name = Prompt.ask("Session name (e.g., 'My University')")
                api_url = Prompt.ask("Canvas URL (e.g., https://canvas.university.edu)")
                api_key = Prompt.ask("API Key", password=True)
                institution_name = Prompt.ask("Institution name (optional)", default="")

                validate = Confirm.ask("Test connection before saving?", default=True)
            else:
                session_name = input("Session name (e.g., 'My University'): ")
                api_url = input("Canvas URL (e.g., https://canvas.university.edu): ")
                api_key = input("API Key: ")
                institution_name = input("Institution name (optional): ")

                validate_input = input("Test connection before saving? (y/n): ").lower()
                validate = validate_input in ['y', 'yes', '']

            # Add session
            success = self.session_manager.add_session(
                session_name=session_name,
                api_url=api_url,
                api_key=api_key,
                institution_name=institution_name,
                validate_connection=validate
            )

            if success:
                self._print_success(f"✅ Session '{session_name}' added successfully!")
            else:
                self._print_error("❌ Failed to add session. Please check your credentials.")

        except Exception as e:
            self.logger.error(f"Failed to add session", exception=e)
            self._print_error(f"Error adding session: {e}")

    async def _connect_to_session(self):
        """Connect to an existing session."""
        sessions = self.session_manager.list_sessions()
        if not sessions:
            self._print_error("No sessions available.")
            return

        self._print_header("Connect to Canvas Session")

        try:
            if self.use_rich:
                session_names = [s['session_name'] for s in sessions]
                session_name = Prompt.ask("Select session", choices=session_names)
            else:
                print("Available Sessions:")
                for i, session in enumerate(sessions, 1):
                    print(f"{i}. {session['session_name']}")
                print()

                while True:
                    try:
                        choice = int(input("Select session number: "))
                        if 1 <= choice <= len(sessions):
                            session_name = sessions[choice - 1]['session_name']
                            break
                        else:
                            print("Invalid choice. Please try again.")
                    except ValueError:
                        print("Please enter a valid number.")

            # Initialize session
            self._print("🔄 Connecting to Canvas...")
            success = await self.orchestrator.initialize_session(session_name)

            if success:
                self.current_session = session_name
                self._print_success(f"✅ Connected to '{session_name}' successfully!")

                # Load available courses
                self._print("📚 Loading available courses...")
                self.available_courses = await self.orchestrator.get_available_courses()
                self._print_success(f"Found {len(self.available_courses)} courses")
            else:
                self._print_error("❌ Failed to connect to session.")

        except Exception as e:
            self.logger.error(f"Failed to connect to session", exception=e)
            self._print_error(f"Connection error: {e}")

    async def _select_courses_menu(self):
        """Handle course selection menu."""
        if not self.current_session:
            self._print_error("❌ Please connect to a Canvas session first.")
            return

        if not self.available_courses:
            self._print("📚 Loading available courses...")
            try:
                self.available_courses = await self.orchestrator.get_available_courses()
            except Exception as e:
                self._print_error(f"Failed to load courses: {e}")
                return

        while True:
            self._clear_screen()
            self._print_header("Course Selection")

            if self.use_rich:
                # Show courses table
                table = Table(title=f"Available Courses ({len(self.available_courses)} total)")
                table.add_column("ID", style="cyan", no_wrap=True)
                table.add_column("Course Name", style="white")
                table.add_column("Code", style="green")
                table.add_column("Term", style="yellow")
                table.add_column("Selected", style="magenta")

                for course in self.available_courses:
                    selected = "✅" if str(course['id']) in self.selected_courses else "⬜"
                    parsed = course.get('parsed')
                    term = f"{parsed.year}-{parsed.semester}" if parsed and parsed.is_parsed_successfully else "Unknown"

                    table.add_row(
                        str(course['id']),
                        course['name'][:50] + "..." if len(course['name']) > 50 else course['name'],
                        course.get('course_code', '')[:10],
                        term,
                        selected
                    )

                self.console.print(table)
                self.console.print()

                # Selection menu
                menu_table = Table(show_header=False, box=None)
                menu_table.add_column("Option", style="cyan", no_wrap=True)
                menu_table.add_column("Description", style="white")

                menu_table.add_row("1", "✅ Select/Deselect Courses")
                menu_table.add_row("2", "🔍 Filter Courses")
                menu_table.add_row("3", "🎯 Select All")
                menu_table.add_row("4", "❌ Clear Selection")
                menu_table.add_row("5", "📊 Check Content Availability")
                menu_table.add_row("0", "⬅️  Back to Main Menu")

                self.console.print(menu_table)
                self.console.print()

                if self.selected_courses:
                    self.console.print(f"Currently selected: {len(self.selected_courses)} courses")
                    self.console.print()

                choice = Prompt.ask("Select an option", choices=["1", "2", "3", "4", "5", "0"])
            else:
                # Basic text interface
                print(f"Available Courses ({len(self.available_courses)} total):")
                print("-" * 50)

                for i, course in enumerate(self.available_courses[:20], 1):  # Show first 20
                    selected = "[X]" if str(course['id']) in self.selected_courses else "[ ]"
                    print(f"{selected} {i:2d}. {course['name'][:40]}")

                if len(self.available_courses) > 20:
                    print(f"... and {len(self.available_courses) - 20} more courses")

                print()
                print(f"Currently selected: {len(self.selected_courses)} courses")
                print()
                print("1. Select/Deselect Courses")
                print("2. Filter Courses")
                print("3. Select All")
                print("4. Clear Selection")
                print("5. Check Content Availability")
                print("0. Back to Main Menu")
                print()
                choice = input("Select an option: ").strip()

            if choice == '1':
                await self._select_individual_courses()
            elif choice == '2':
                await self._filter_courses()
            elif choice == '3':
                self._select_all_courses()
            elif choice == '4':
                self._clear_course_selection()
            elif choice == '5':
                await self._check_content_availability()
            elif choice == '0':
                break
            else:
                self._print_error("Invalid option.")

            if choice != '0':
                self._wait_for_enter()

    async def _select_individual_courses(self):
        """Allow individual course selection."""
        self._print_header("Select Individual Courses")

        try:
            if self.use_rich:
                # Show course selection interface
                course_choices = []
                for course in self.available_courses:
                    parsed = course.get('parsed')
                    term = f"{parsed.year}-{parsed.semester}" if parsed and parsed.is_parsed_successfully else "Unknown"
                    choice_text = f"{course['name']} ({course.get('course_code', '')}) - {term}"
                    course_choices.append((str(course['id']), choice_text))

                self.console.print("Enter course IDs to toggle selection (comma-separated):")
                self.console.print("Example: 12345,67890,54321")
                self.console.print()

                course_ids_input = Prompt.ask("Course IDs")
                course_ids = [cid.strip() for cid in course_ids_input.split(',') if cid.strip()]
            else:
                print("Enter course numbers to toggle selection (comma-separated):")
                print("Example: 1,3,5")
                print()

                selection_input = input("Course numbers: ")
                course_numbers = [int(n.strip()) for n in selection_input.split(',') if n.strip().isdigit()]
                course_ids = [str(self.available_courses[n - 1]['id']) for n in course_numbers
                              if 1 <= n <= len(self.available_courses)]

            # Toggle selection
            for course_id in course_ids:
                if course_id in self.selected_courses:
                    self.selected_courses.remove(course_id)
                    self._print(f"➖ Deselected course {course_id}")
                else:
                    self.selected_courses.append(course_id)
                    self._print(f"➕ Selected course {course_id}")

            self._print_success(f"✅ Selection updated. {len(self.selected_courses)} courses selected.")

        except Exception as e:
            self._print_error(f"Error in course selection: {e}")

    def _select_all_courses(self):
        """Select all available courses."""
        self.selected_courses = [str(course['id']) for course in self.available_courses]
        self._print_success(f"✅ Selected all {len(self.selected_courses)} courses.")

    def _clear_course_selection(self):
        """Clear all course selections."""
        self.selected_courses = []
        self._print_success("✅ Cleared course selection.")

    async def _configure_content_types_menu(self):
        """Handle content type configuration."""
        content_types = [
            ('announcements', '📢 Announcements'),
            ('assignments', '📝 Assignments'),
            ('discussions', '💬 Discussions'),
            ('files', '📁 Files'),
            ('modules', '📚 Modules'),
            ('quizzes', '❓ Quizzes'),
            ('grades', '📊 Grades'),
            ('people', '👥 People'),
            ('chat', '💭 Chat')
        ]

        while True:
            self._clear_screen()
            self._print_header("Configure Content Types")

            if self.use_rich:
                # Show content types table
                table = Table(title="Available Content Types")
                table.add_column("Type", style="cyan")
                table.add_column("Description", style="white")
                table.add_column("Status", style="green")

                for content_type, description in content_types:
                    status = "✅ Enabled" if content_type in self.enabled_content_types else "❌ Disabled"
                    table.add_row(content_type, description, status)

                self.console.print(table)
                self.console.print()

                # Menu options
                menu_table = Table(show_header=False, box=None)
                menu_table.add_column("Option", style="cyan", no_wrap=True)
                menu_table.add_column("Description", style="white")

                menu_table.add_row("1", "🔧 Toggle Individual Types")
                menu_table.add_row("2", "✅ Enable All")
                menu_table.add_row("3", "❌ Disable All")
                menu_table.add_row("4", "📋 Use Preset: Academic Content")
                menu_table.add_row("5", "📋 Use Preset: Communication Only")
                menu_table.add_row("0", "⬅️  Back to Main Menu")

                self.console.print(menu_table)
                self.console.print()

                choice = Prompt.ask("Select an option", choices=["1", "2", "3", "4", "5", "0"])
            else:
                # Basic text interface
                print("Content Types:")
                print("-" * 30)

                for i, (content_type, description) in enumerate(content_types, 1):
                    status = "[X]" if content_type in self.enabled_content_types else "[ ]"
                    print(f"{status} {i}. {description}")

                print()
                print("1. Toggle Individual Types")
                print("2. Enable All")
                print("3. Disable All")
                print("4. Use Preset: Academic Content")
                print("5. Use Preset: Communication Only")
                print("0. Back to Main Menu")
                print()
                choice = input("Select an option: ").strip()

            if choice == '1':
                await self._toggle_content_types(content_types)
            elif choice == '2':
                self._enable_all_content_types(content_types)
            elif choice == '3':
                self._disable_all_content_types()
            elif choice == '4':
                self._use_academic_preset()
            elif choice == '5':
                self._use_communication_preset()
            elif choice == '0':
                break
            else:
                self._print_error("Invalid option.")

            if choice != '0':
                self._wait_for_enter()

    async def _toggle_content_types(self, content_types):
        """Toggle individual content types."""
        self._print_header("Toggle Content Types")

        try:
            if self.use_rich:
                type_choices = [ct[0] for ct in content_types]
                selected_types = Prompt.ask(
                    "Enter content types to toggle (comma-separated)",
                    default=""
                ).split(',')
                selected_types = [t.strip() for t in selected_types if t.strip()]
            else:
                print("Enter numbers to toggle (comma-separated):")
                selection_input = input("Numbers: ")
                selected_numbers = [int(n.strip()) for n in selection_input.split(',')
                                    if n.strip().isdigit()]
                selected_types = [content_types[n - 1][0] for n in selected_numbers
                                  if 1 <= n <= len(content_types)]

            # Toggle selected types
            for content_type in selected_types:
                if content_type in [ct[0] for ct in content_types]:
                    if content_type in self.enabled_content_types:
                        self.enabled_content_types.remove(content_type)
                        self._print(f"➖ Disabled {content_type}")
                    else:
                        self.enabled_content_types.add(content_type)
                        self._print(f"➕ Enabled {content_type}")

            self._print_success(f"✅ Updated content types. {len(self.enabled_content_types)} enabled.")

        except Exception as e:
            self._print_error(f"Error updating content types: {e}")

    def _enable_all_content_types(self, content_types):
        """Enable all content types."""
        self.enabled_content_types = {ct[0] for ct in content_types}
        self._print_success("✅ Enabled all content types.")

    def _disable_all_content_types(self):
        """Disable all content types."""
        self.enabled_content_types.clear()
        self._print_success("✅ Disabled all content types.")

    def _use_academic_preset(self):
        """Use academic content preset."""
        self.enabled_content_types = {
            'announcements', 'assignments', 'discussions',
            'files', 'modules', 'quizzes', 'grades'
        }
        self._print_success("✅ Applied Academic Content preset.")

    def _use_communication_preset(self):
        """Use communication-only preset."""
        self.enabled_content_types = {'announcements', 'discussions', 'chat'}
        self._print_success("✅ Applied Communication Only preset.")

    async def _start_download(self):
        """Start the download process."""
        # Validate prerequisites
        if not self.current_session:
            self._print_error("❌ Please connect to a Canvas session first.")
            return

        if not self.selected_courses:
            self._print_error("❌ Please select at least one course.")
            return

        if not self.enabled_content_types:
            self._print_error("❌ Please enable at least one content type.")
            return

        # Show download confirmation
        self._clear_screen()
        self._print_header("Download Confirmation")

        if self.use_rich:
            # Create confirmation panel
            info_text = f"""
📡 Session: {self.current_session}
📚 Courses: {len(self.selected_courses)} selected
📋 Content Types: {', '.join(sorted(self.enabled_content_types))}
📁 Download Path: {self.config.get_download_path()}
            """.strip()

            panel = Panel(info_text, title="Download Configuration", border_style="yellow")
            self.console.print(panel)
            self.console.print()

            if not Confirm.ask("Start download?", default=True):
                self._print("❌ Download cancelled.")
                return
        else:
            print("Download Configuration:")
            print(f"Session: {self.current_session}")
            print(f"Courses: {len(self.selected_courses)} selected")
            print(f"Content Types: {', '.join(sorted(self.enabled_content_types))}")
            print(f"Download Path: {self.config.get_download_path()}")
            print()

            confirm = input("Start download? (y/n): ").lower()
            if confirm not in ['y', 'yes']:
                print("Download cancelled.")
                return

        # Configure orchestrator
        self.orchestrator.configure_content_types(list(self.enabled_content_types))

        # Start download with progress tracking
        try:
            self.is_downloading = True
            self._print("🚀 Starting download...")

            # Create progress tracker
            progress_tracker = create_progress_tracker(use_rich=self.use_rich)

            # Run download
            results = await self.orchestrator.download_courses(
                self.selected_courses,
                progress_callback=self._progress_callback
            )

            # Show results
            await self._show_download_results(results)

        except Exception as e:
            self.logger.error(f"Download failed", exception=e)
            self._print_error(f"❌ Download failed: {e}")
        finally:
            self.is_downloading = False

    def _progress_callback(self, level: str, progress_state):
        """Handle progress updates during download."""
        # This could be used to update a live progress display
        pass

    async def _show_download_results(self, results: DownloadResults):
        """Show download completion results."""
        self._clear_screen()
        self._print_header("Download Complete!")

        if self.use_rich:
            # Create results table
            table = Table(title="Download Results")
            table.add_column("Metric", style="cyan")
            table.add_column("Value", style="green")

            table.add_row("Total Courses", str(results.total_courses))
            table.add_row("Successful", str(results.successful_courses))
            table.add_row("Failed", str(results.failed_courses))
            table.add_row("Success Rate", f"{results.success_rate:.1f}%")
            table.add_row("Total Items", str(results.total_content_items))
            table.add_row("Downloaded", str(results.downloaded_items))
            table.add_row("Files Downloaded", str(results.total_files_downloaded))
            table.add_row("Total Size", f"{results.total_bytes_downloaded / (1024 * 1024):.1f} MB")
            table.add_row("Duration", results._format_duration(results.duration))
            table.add_row("Speed", f"{results.download_speed_mb_per_sec:.2f} MB/s")

            self.console.print(table)
            self.console.print()

            if results.download_paths:
                paths_panel = Panel(
                    "\n".join(results.download_paths[:5]) +
                    (f"\n... and {len(results.download_paths) - 5} more" if len(results.download_paths) > 5 else ""),
                    title="Download Locations",
                    border_style="green"
                )
                self.console.print(paths_panel)
                self.console.print()
        else:
            print("Download Results:")
            print("-" * 30)
            print(f"Total Courses: {results.total_courses}")
            print(f"Successful: {results.successful_courses}")
            print(f"Failed: {results.failed_courses}")
            print(f"Success Rate: {results.success_rate:.1f}%")
            print(f"Total Items: {results.total_content_items}")
            print(f"Downloaded: {results.downloaded_items}")
            print(f"Files Downloaded: {results.total_files_downloaded}")
            print(f"Total Size: {results.total_bytes_downloaded / (1024 * 1024):.1f} MB")
            print(f"Duration: {results._format_duration(results.duration)}")
            print(f"Speed: {results.download_speed_mb_per_sec:.2f} MB/s")
            print()

            if results.download_paths:
                print("Download Locations:")
                for path in results.download_paths[:3]:
                    print(f"  {path}")
                if len(results.download_paths) > 3:
                    print(f"  ... and {len(results.download_paths) - 3} more")
                print()

        if results.errors:
            self._print_error(f"⚠️  {len(results.errors)} errors occurred during download.")
            if self.use_rich:
                show_errors = Confirm.ask("Show error details?", default=False)
            else:
                show_errors = input("Show error details? (y/n): ").lower() in ['y', 'yes']

            if show_errors:
                self._show_error_details(results.errors)

    def _show_error_details(self, errors: List[Dict[str, Any]]):
        """Show detailed error information."""
        self._print_header("Error Details")

        for i, error in enumerate(errors[:10], 1):  # Show first 10 errors
            print(f"{i}. {error.get('error_type', 'Unknown')}: {error.get('error_message', 'No message')}")
            if error.get('course_id'):
                print(f"   Course ID: {error['course_id']}")
            if error.get('content_type'):
                print(f"   Content Type: {error['content_type']}")
            print(f"   Time: {error.get('timestamp', 'Unknown')}")
            print()

        if len(errors) > 10:
            print(f"... and {len(errors) - 10} more errors")

    # Utility methods
    def _clear_screen(self):
        """Clear the console screen."""
        if self.use_rich:
            self.console.clear()
        else:
            import os
            os.system('cls' if os.name == 'nt' else 'clear')

    def _print_header(self, title: str):
        """Print a formatted header."""
        if self.use_rich:
            self.console.print(f"\n[bold blue]{title}[/bold blue]")
            self.console.print("=" * len(title))
            self.console.print()
        else:
            print(f"\n{title}")
            print("=" * len(title))
            print()

    def _print(self, message: str):
        """Print a message."""
        if self.use_rich:
            self.console.print(message)
        else:
            print(message)

    def _print_success(self, message: str):
        """Print a success message."""
        if self.use_rich:
            self.console.print(f"[green]{message}[/green]")
        else:
            print(message)

    def _print_error(self, message: str):
        """Print an error message."""
        if self.use_rich:
            self.console.print(f"[red]{message}[/red]")
        else:
            print(f"ERROR: {message}")

    def _wait_for_enter(self):
        """Wait for user to press Enter."""
        if self.use_rich:
            Prompt.ask("\nPress Enter to continue", default="")
        else:
            input("\nPress Enter to continue...")

    def _show_welcome(self):
        """Show welcome message."""
        if self.use_rich:
            welcome_text = """
# 🎓 Canvas Downloader

Welcome to Canvas Downloader! This tool helps you download and organize 
content from your Canvas courses including assignments, announcements, 
discussions, files, and more.

## Getting Started
1. **Add a Canvas Session**: Set up your Canvas URL and API key
2. **Select Courses**: Choose which courses to download
3. **Configure Content**: Select what types of content to download
4. **Start Download**: Begin the download process

Your downloads will be organized by course and semester for easy browsing.
            """

            welcome_panel = Panel(
                Markdown(welcome_text),
                title="Welcome",
                border_style="blue",
                expand=False
            )
            self.console.print(welcome_panel)
            self.console.print()
        else:
            print("=" * 60)
            print("Canvas Downloader")
            print("=" * 60)
            print("Welcome! This tool helps you download content from Canvas courses.")
            print()

    def _show_help(self):
        """Show help information."""
        self._clear_screen()
        self._print_header("Help & Documentation")

        if self.use_rich:
            help_text = """
# Canvas Downloader Help

## API Key Setup
1. Log into your Canvas account
2. Go to Account → Settings
3. Scroll to "Approved Integrations"
4. Click "+ New Access Token"
5. Enter a purpose and click "Generate Token"
6. Copy the token - you won't see it again!

## Content Types
- **Announcements**: Course announcements and updates
- **Assignments**: Assignment descriptions, rubrics, and files
- **Discussions**: Discussion forums and replies
- **Files**: All course files and documents
- **Modules**: Course modules and lesson content
- **Quizzes**: Quiz content and questions
- **Grades**: Grade information and feedback
- **People**: Course participant information

## File Organization
Downloads are organized as:
```
Year/Semester/Course Name/Content Type/
```

For example:
```
2024/1 Semester/Math 101 Calculus/assignments/
2024/1 Semester/Math 101 Calculus/files/
```

## Tips
- Start with a few courses to test your setup
- Use "Academic Content" preset for most use cases
- Check content availability before downloading
- Large courses may take significant time to download
            """

            help_panel = Panel(
                Markdown(help_text),
                title="Help",
                border_style="green"
            )
            self.console.print(help_panel)
        else:
            print("Canvas Downloader Help")
            print("-" * 30)
            print()
            print("API Key Setup:")
            print("1. Log into Canvas → Account → Settings")
            print("2. Scroll to 'Approved Integrations'")
            print("3. Click '+ New Access Token'")
            print("4. Generate and copy your token")
            print()
            print("Content Types:")
            print("- Announcements: Course updates")
            print("- Assignments: Tasks and rubrics")
            print("- Discussions: Forum conversations")
            print("- Files: Course documents")
            print("- Modules: Lesson content")
            print("- Quizzes: Quiz content")
            print("- Grades: Grade information")
            print("- People: Participant info")
            print()
            print("Files are organized by year/semester/course/type")

    # Placeholder methods for remaining functionality
    async def _configure_settings_menu(self):
        """Configure application settings."""
        self._print("⚙️ Settings configuration coming soon!")

    async def _view_download_status(self):
        """View current download status."""
        if self.is_downloading:
            self._print("📊 Download in progress...")
            # Show real-time status
        else:
            self._print("📊 No download currently in progress.")

    async def _view_download_history(self):
        """View download history."""
        self._print("📈 Download history coming soon!")

    async def _filter_courses(self):
        """Filter available courses."""
        self._print("🔍 Course filtering coming soon!")

    async def _check_content_availability(self):
        """Check content availability for selected courses."""
        if not self.selected_courses:
            self._print_error("No courses selected.")
            return

        self._print("🔍 Checking content availability...")
        try:
            availability = await self.orchestrator.check_course_content_availability(self.selected_courses)
            self._print_success(f"✅ Checked {len(availability)} courses.")
        except Exception as e:
            self._print_error(f"Failed to check availability: {e}")

    async def _edit_session(self):
        """Edit an existing session."""
        self._print("✏️ Session editing coming soon!")

    async def _delete_session(self):
        """Delete an existing session."""
        self._print("🗑️ Session deletion coming soon!")

    async def _test_connection(self):
        """Test connection to current session."""
        if not self.current_session:
            self._print_error("No session selected.")
            return

        self._print("🔍 Testing connection...")
        try:
            # Re-initialize to test
            success = await self.orchestrator.initialize_session(self.current_session)
            if success:
                self._print_success("✅ Connection successful!")
            else:
                self._print_error("❌ Connection failed.")
        except Exception as e:
            self._print_error(f"Connection test failed: {e}")

    async def _cleanup(self):
        """Clean up CLI resources."""
        try:
            if self.orchestrator:
                await self.orchestrator.cleanup()
        except Exception as e:
            self.logger.error(f"Cleanup error", exception=e)


def main() -> int:
    """
    Main entry point for the CLI application.

    Returns:
        int: Exit code
    """
    try:
        cli = CanvasDownloaderCLI()
        return cli.run()
    except Exception as e:
        print(f"Failed to start application: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
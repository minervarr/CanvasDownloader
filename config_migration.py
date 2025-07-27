#!/usr/bin/env python3
"""
Configuration Migration and Validation Script

This script performs comprehensive migration and validation of the Canvas
Downloader configuration system. It ensures that all configuration files
are properly structured and that the codebase is compatible with the new
bulletproof configuration system.

Features:
- Validates existing configuration files
- Migrates old configuration formats to new format
- Checks all Python files for configuration access patterns
- Updates incompatible access patterns
- Creates backup copies of all modified files
- Generates detailed migration report

Usage:
    python config_migration.py [--dry-run] [--backup-dir BACKUP_DIR]

    --dry-run: Show what would be changed without making changes
    --backup-dir: Directory to store backups (default: ./backups)
"""

import os
import sys
import json
import shutil
import argparse
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Any
import ast


class ConfigMigrationTool:
    """
    Configuration Migration and Validation Tool

    This tool helps migrate from the old configuration system to the new
    bulletproof configuration system, ensuring compatibility and providing
    detailed reporting.
    """

    def __init__(self, dry_run: bool = False, backup_dir: str = "backups"):
        """
        Initialize the migration tool.

        Args:
            dry_run: If True, only show what would be changed
            backup_dir: Directory to store backups
        """
        self.dry_run = dry_run
        self.backup_dir = Path(backup_dir)
        self.migration_report = {
            'timestamp': datetime.now().isoformat(),
            'dry_run': dry_run,
            'files_checked': 0,
            'files_modified': 0,
            'config_files_migrated': 0,
            'python_files_updated': 0,
            'issues_found': [],
            'changes_made': [],
            'warnings': [],
            'errors': []
        }

        # Patterns to find problematic configuration access
        self.problematic_patterns = [
            # Direct attribute access to config sections
            r'(\w+\.)?config\.download_settings\.(\w+)',
            r'(\w+\.)?config\.canvas_api\.(\w+)',
            r'(\w+\.)?config\.content_types\.(\w+)',
            r'(\w+\.)?config\.folder_structure\.(\w+)',
            r'(\w+\.)?config\.paths\.(\w+)',
            r'(\w+\.)?config\.logging\.(\w+)',
            r'(\w+\.)?config\.ui\.(\w+)',
            r'(\w+\.)?config\.performance\.(\w+)',
            r'(\w+\.)?config\.security\.(\w+)',
        ]

        # Replacement patterns for safer access
        self.replacement_patterns = {
            r'config\.download_settings\.(\w+)': r"config.safe_get('download_settings.\1', DEFAULT_VALUE, EXPECTED_TYPE)",
            r'config\.canvas_api\.(\w+)': r"config.safe_get('canvas_api.\1', DEFAULT_VALUE, EXPECTED_TYPE)",
            r'config\.content_types\.(\w+)': r"config.safe_get('content_types.\1', DEFAULT_VALUE, EXPECTED_TYPE)",
            r'config\.folder_structure\.(\w+)': r"config.safe_get('folder_structure.\1', DEFAULT_VALUE, EXPECTED_TYPE)",
            r'config\.paths\.(\w+)': r"config.safe_get('paths.\1', DEFAULT_VALUE, EXPECTED_TYPE)",
            r'config\.logging\.(\w+)': r"config.safe_get('logging.\1', DEFAULT_VALUE, EXPECTED_TYPE)",
            r'config\.ui\.(\w+)': r"config.safe_get('ui.\1', DEFAULT_VALUE, EXPECTED_TYPE)",
            r'config\.performance\.(\w+)': r"config.safe_get('performance.\1', DEFAULT_VALUE, EXPECTED_TYPE)",
            r'config\.security\.(\w+)': r"config.safe_get('security.\1', DEFAULT_VALUE, EXPECTED_TYPE)",
        }

        # Expected configuration structure
        self.expected_config_structure = {
            'download_settings': {
                'max_retries': 3,
                'retry_delay': 1.0,
                'chunk_size': 8192,
                'timeout': 30,
                'verify_downloads': True,
                'skip_existing': True,
                'parallel_downloads': 4,
                'max_file_size_mb': 500,
                'base_download_path': 'downloads',
                'allowed_extensions': [],
                'blocked_extensions': ['.exe', '.bat', '.cmd', '.scr']
            },
            'canvas_api': {
                'timeout': 30,
                'max_retries': 3,
                'retry_delay': 1.0,
                'rate_limit_delay': 0.1,
                'verify_ssl': True,
                'user_agent': 'Canvas-Downloader/0.1.0'
            },
            'content_types': {
                'modules': {
                    'enabled': True,
                    'priority': 1,
                    'download_module_content': True,
                    'download_module_items': True,
                    'download_associated_files': True,
                    'create_module_index': True
                },
                'assignments': {
                    'enabled': True,
                    'priority': 2,
                    'download_instructions': True,
                    'download_attachments': True,
                    'download_rubrics': True,
                    'download_submissions': False,
                    'organize_by_groups': True,
                    'convert_html_to_markdown': True
                }
            },
            'folder_structure': {
                'organize_by_semester': True,
                'create_assignment_groups': True,
                'include_due_dates': True,
                'sanitize_names': True,
                'max_folder_depth': 10,
                'folder_name_template': '{course_code}-{course_name}'
            },
            'paths': {
                'downloads_folder': 'downloads',
                'config_folder': 'config',
                'logs_folder': 'logs',
                'temp_folder': 'temp',
                'cache_folder': 'cache'
            }
        }

    def run_migration(self, project_root: Path = None) -> Dict[str, Any]:
        """
        Run the complete migration process.

        Args:
            project_root: Root directory of the project

        Returns:
            Dict[str, Any]: Migration report
        """
        if project_root is None:
            project_root = Path.cwd()

        print(f"🚀 Starting Canvas Downloader Configuration Migration")
        print(f"📁 Project root: {project_root}")
        print(f"🔧 Mode: {'DRY RUN' if self.dry_run else 'LIVE MIGRATION'}")
        print(f"💾 Backup directory: {self.backup_dir}")
        print("=" * 60)

        try:
            # Create backup directory
            if not self.dry_run:
                self.backup_dir.mkdir(parents=True, exist_ok=True)

            # Step 1: Migrate configuration files
            print("\n📋 Step 1: Migrating configuration files...")
            self._migrate_config_files(project_root)

            # Step 2: Update Python files
            print("\n🐍 Step 2: Updating Python files...")
            self._update_python_files(project_root)

            # Step 3: Validate the migration
            print("\n✅ Step 3: Validating migration...")
            self._validate_migration(project_root)

            # Generate final report
            self._generate_final_report()

        except Exception as e:
            self.migration_report['errors'].append(f"Migration failed: {str(e)}")
            print(f"❌ Migration failed: {e}")

        return self.migration_report

    def _migrate_config_files(self, project_root: Path) -> None:
        """Migrate configuration files to new format."""
        config_files = [
            project_root / "config" / "config.json",
            project_root / "config.json",
            project_root / "canvas_config.json"
        ]

        for config_file in config_files:
            if config_file.exists():
                print(f"  📄 Found config file: {config_file}")
                self._migrate_single_config_file(config_file)

    def _migrate_single_config_file(self, config_file: Path) -> None:
        """Migrate a single configuration file."""
        try:
            # Load existing config
            with open(config_file, 'r', encoding='utf-8') as f:
                current_config = json.load(f)

            # Create backup
            if not self.dry_run:
                backup_path = self.backup_dir / f"{config_file.name}.backup"
                shutil.copy2(config_file, backup_path)
                print(f"    💾 Created backup: {backup_path}")

            # Migrate config structure
            migrated_config = self._merge_with_expected_structure(current_config)

            # Check for missing fields
            missing_fields = self._find_missing_fields(current_config)
            if missing_fields:
                print(f"    ➕ Adding missing fields: {', '.join(missing_fields)}")
                self.migration_report['changes_made'].append(
                    f"Added missing fields to {config_file}: {missing_fields}"
                )

            # Save migrated config
            if not self.dry_run:
                with open(config_file, 'w', encoding='utf-8') as f:
                    json.dump(migrated_config, f, indent=2, ensure_ascii=False)
                print(f"    ✅ Migrated: {config_file}")
            else:
                print(f"    🔍 Would migrate: {config_file}")

            self.migration_report['config_files_migrated'] += 1

        except Exception as e:
            error_msg = f"Failed to migrate {config_file}: {str(e)}"
            self.migration_report['errors'].append(error_msg)
            print(f"    ❌ {error_msg}")

    def _merge_with_expected_structure(self, current_config: Dict[str, Any]) -> Dict[str, Any]:
        """Merge current config with expected structure."""

        def merge_dicts(default: Dict, current: Dict) -> Dict:
            result = default.copy()
            for key, value in current.items():
                if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                    result[key] = merge_dicts(result[key], value)
                else:
                    result[key] = value
            return result

        return merge_dicts(self.expected_config_structure, current_config)

    def _find_missing_fields(self, current_config: Dict[str, Any]) -> List[str]:
        """Find missing fields in current configuration."""
        missing = []

        def check_missing(expected: Dict, current: Dict, prefix: str = "") -> None:
            for key, value in expected.items():
                current_key = f"{prefix}.{key}" if prefix else key

                if key not in current:
                    missing.append(current_key)
                elif isinstance(value, dict) and isinstance(current.get(key), dict):
                    check_missing(value, current[key], current_key)

        check_missing(self.expected_config_structure, current_config)
        return missing

    def _update_python_files(self, project_root: Path) -> None:
        """Update Python files with problematic configuration access patterns."""
        python_files = list(project_root.rglob("*.py"))

        for py_file in python_files:
            # Skip certain directories
            if any(skip_dir in str(py_file) for skip_dir in ['.git', '__pycache__', '.pytest_cache', 'venv', 'env']):
                continue

            self._update_single_python_file(py_file)

    def _update_single_python_file(self, py_file: Path) -> None:
        """Update a single Python file."""
        try:
            self.migration_report['files_checked'] += 1

            with open(py_file, 'r', encoding='utf-8') as f:
                content = f.read()

            original_content = content
            issues_found = []

            # Check for problematic patterns
            for pattern in self.problematic_patterns:
                matches = re.finditer(pattern, content)
                for match in matches:
                    line_num = content[:match.start()].count('\n') + 1
                    issue = {
                        'file': str(py_file),
                        'line': line_num,
                        'pattern': match.group(),
                        'issue': 'Direct attribute access to config section'
                    }
                    issues_found.append(issue)
                    self.migration_report['issues_found'].append(issue)

            if issues_found:
                print(f"  🔍 Found {len(issues_found)} issues in: {py_file}")

                if not self.dry_run:
                    # Create backup
                    backup_path = self.backup_dir / f"{py_file.name}.backup"
                    shutil.copy2(py_file, backup_path)

                # Show suggested fixes
                self._show_suggested_fixes(py_file, issues_found)

                # For specific known files, we can apply automatic fixes
                if py_file.name in ['file_manager.py', 'orchestrator.py']:
                    updated_content = self._apply_automatic_fixes(py_file, content)

                    if updated_content != original_content:
                        if not self.dry_run:
                            with open(py_file, 'w', encoding='utf-8') as f:
                                f.write(updated_content)
                            print(f"    ✅ Applied automatic fixes to: {py_file}")
                        else:
                            print(f"    🔍 Would apply automatic fixes to: {py_file}")

                        self.migration_report['files_modified'] += 1
                        self.migration_report['python_files_updated'] += 1

        except Exception as e:
            error_msg = f"Failed to process {py_file}: {str(e)}"
            self.migration_report['errors'].append(error_msg)
            print(f"    ❌ {error_msg}")

    def _show_suggested_fixes(self, py_file: Path, issues: List[Dict[str, Any]]) -> None:
        """Show suggested fixes for issues found."""
        print(f"    📝 Suggested fixes for {py_file.name}:")

        for issue in issues[:3]:  # Show first 3 issues
            line_num = issue['line']
            pattern = issue['pattern']

            # Generate suggested fix
            if 'download_settings' in pattern:
                section = 'download_settings'
                field = pattern.split('.')[-1]
                suggestion = f"config.safe_get('{section}.{field}', DEFAULT_VALUE, EXPECTED_TYPE)"
            elif any(section in pattern for section in ['canvas_api', 'content_types', 'paths']):
                # Extract section and field
                parts = pattern.split('.')
                if len(parts) >= 3:
                    section = parts[-2]
                    field = parts[-1]
                    suggestion = f"config.safe_get('{section}.{field}', DEFAULT_VALUE, EXPECTED_TYPE)"
                else:
                    suggestion = "Use config.safe_get() method with appropriate defaults"
            else:
                suggestion = "Use config.safe_get() method with appropriate defaults"

            print(f"      Line {line_num}: {pattern}")
            print(f"      Suggested: {suggestion}")
            print()

    def _apply_automatic_fixes(self, py_file: Path, content: str) -> str:
        """Apply automatic fixes for known files."""
        if py_file.name == 'file_manager.py':
            return self._fix_file_manager(content)
        elif py_file.name == 'orchestrator.py':
            return self._fix_orchestrator(content)
        else:
            return content

    def _fix_file_manager(self, content: str) -> str:
        """Fix file_manager.py configuration access."""
        fixes = [
            (
                r'self\.base_download_path = Path\(self\.config\.download_settings\.base_download_path\)',
                "self.base_download_path = Path(self.config.safe_get('download_settings.base_download_path', 'downloads', str))"
            ),
            (
                r'self\.chunk_size = self\.config\.download_settings\.chunk_size',
                "self.chunk_size = self.config.safe_get('download_settings.chunk_size', 8192, int)"
            ),
            (
                r'self\.config\.download_settings\.(\w+)',
                r"self.config.safe_get('download_settings.\1', DEFAULT_VALUE, EXPECTED_TYPE)"
            )
        ]

        for pattern, replacement in fixes:
            content = re.sub(pattern, replacement, content)

        return content

    def _fix_orchestrator(self, content: str) -> str:
        """Fix orchestrator.py configuration access."""
        fixes = [
            (
                r'self\.config\.download_settings\.parallel_downloads',
                "self.config.safe_get('download_settings.parallel_downloads', 4, int)"
            ),
            (
                r'self\.config\.download_settings\.skip_existing',
                "self.config.safe_get('download_settings.skip_existing', True, bool)"
            )
        ]

        for pattern, replacement in fixes:
            content = re.sub(pattern, replacement, content)

        return content

    def _validate_migration(self, project_root: Path) -> None:
        """Validate the migration results."""
        print("  🔍 Checking for remaining issues...")

        # Try to import and test the new configuration
        try:
            sys.path.insert(0, str(project_root / 'src'))
            from src.config.settings import get_config, validate_config_file

            # Test configuration loading
            config = get_config()
            print("    ✅ Configuration loads successfully")

            # Test attribute access
            try:
                download_settings = config.download_settings
                max_retries = download_settings.max_retries
                print(f"    ✅ Attribute access works: max_retries = {max_retries}")
            except Exception as e:
                self.migration_report['warnings'].append(f"Attribute access test failed: {e}")
                print(f"    ⚠️  Attribute access test failed: {e}")

            # Test safe_get method
            try:
                chunk_size = config.safe_get('download_settings.chunk_size', 8192, int)
                print(f"    ✅ safe_get method works: chunk_size = {chunk_size}")
            except Exception as e:
                self.migration_report['warnings'].append(f"safe_get method test failed: {e}")
                print(f"    ⚠️  safe_get method test failed: {e}")

        except ImportError as e:
            self.migration_report['warnings'].append(f"Could not import new configuration: {e}")
            print(f"    ⚠️  Could not import new configuration: {e}")
        except Exception as e:
            self.migration_report['errors'].append(f"Configuration validation failed: {e}")
            print(f"    ❌ Configuration validation failed: {e}")

    def _generate_final_report(self) -> None:
        """Generate and display the final migration report."""
        print("\n" + "=" * 60)
        print("📊 MIGRATION REPORT")
        print("=" * 60)

        # Summary statistics
        print(f"📁 Files checked: {self.migration_report['files_checked']}")
        print(f"📄 Config files migrated: {self.migration_report['config_files_migrated']}")
        print(f"🐍 Python files updated: {self.migration_report['python_files_updated']}")
        print(f"🔧 Total files modified: {self.migration_report['files_modified']}")
        print(f"⚠️  Warnings: {len(self.migration_report['warnings'])}")
        print(f"❌ Errors: {len(self.migration_report['errors'])}")

        # Issues found
        if self.migration_report['issues_found']:
            print(f"\n🔍 Issues found: {len(self.migration_report['issues_found'])}")

            # Group by file
            issues_by_file = {}
            for issue in self.migration_report['issues_found']:
                file_name = Path(issue['file']).name
                if file_name not in issues_by_file:
                    issues_by_file[file_name] = []
                issues_by_file[file_name].append(issue)

            for file_name, file_issues in issues_by_file.items():
                print(f"  📄 {file_name}: {len(file_issues)} issues")

        # Warnings and errors
        if self.migration_report['warnings']:
            print(f"\n⚠️  Warnings:")
            for warning in self.migration_report['warnings']:
                print(f"  • {warning}")

        if self.migration_report['errors']:
            print(f"\n❌ Errors:")
            for error in self.migration_report['errors']:
                print(f"  • {error}")

        # Next steps
        print(f"\n🎯 NEXT STEPS:")
        if self.dry_run:
            print("  1. Review the issues found above")
            print("  2. Run migration without --dry-run to apply changes")
            print("  3. Test the application thoroughly")
        else:
            print("  1. Test the application with: python main.py")
            print("  2. Review any remaining warnings or errors")
            print("  3. Update any custom code that wasn't automatically fixed")

        print(f"\n📄 Backup files stored in: {self.backup_dir}")
        print("🎉 Migration completed!")

        # Save report to file
        if not self.dry_run:
            report_file = self.backup_dir / f"migration_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(self.migration_report, f, indent=2, ensure_ascii=False)
            print(f"📋 Detailed report saved to: {report_file}")


def main():
    """Main entry point for the migration script."""
    parser = argparse.ArgumentParser(
        description="Canvas Downloader Configuration Migration Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be changed without making changes'
    )

    parser.add_argument(
        '--backup-dir',
        default='backups',
        help='Directory to store backups (default: ./backups)'
    )

    parser.add_argument(
        '--project-root',
        type=Path,
        default=Path.cwd(),
        help='Root directory of the project (default: current directory)'
    )

    args = parser.parse_args()

    # Create migration tool
    migration_tool = ConfigMigrationTool(
        dry_run=args.dry_run,
        backup_dir=args.backup_dir
    )

    # Run migration
    report = migration_tool.run_migration(args.project_root)

    # Exit with appropriate code
    if report['errors']:
        sys.exit(1)
    elif report['warnings']:
        sys.exit(2)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
Quick script to check if create_course_parser is added correctly.
"""

import sys
import ast
import inspect


def check_course_parser_file():
    """Check if the create_course_parser function is properly defined."""
    try:
        # Try to import the module
        from src.core.course_parser import CourseParser, create_course_parser
        print("✅ Successfully imported both CourseParser and create_course_parser")

        # Check if it's a function
        if inspect.isfunction(create_course_parser):
            print("✅ create_course_parser is a function")

            # Try to call it
            parser = create_course_parser()
            if isinstance(parser, CourseParser):
                print("✅ create_course_parser returns a CourseParser instance")
                return True
            else:
                print("❌ create_course_parser doesn't return a CourseParser instance")
                return False
        else:
            print("❌ create_course_parser is not a function")
            return False

    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def show_file_structure():
    """Show how the file should be structured."""
    print("\n📋 Your course_parser.py file should be structured like this:")
    print("""
    # File: src/core/course_parser.py

    from __future__ import annotations  # Optional - add this at the top
    import re
    from datetime import datetime, timezone
    # ... other imports ...

    @dataclass
    class ParsedCourse:
        # ... class definition ...

    class CourseParser:
        def __init__(self):
            # ... init code ...

        def parse_course(self, canvas_course) -> ParsedCourse:
            # ... method code ...

        # ... other methods ...

        def get_parsing_statistics(self) -> Dict[str, Any]:
            # ... last method of the class ...

    # ⬇️ ADD THE FUNCTION HERE (at module level, NOT indented)
    def create_course_parser(config=None) -> 'CourseParser':  # or -> CourseParser if using future annotations
        return CourseParser()
    """)


if __name__ == "__main__":
    print("🔍 Checking create_course_parser placement...")
    print("=" * 50)

    if check_course_parser_file():
        print("\n✅ Everything looks good!")
        print("You can now run: python main.py")
    else:
        print("\n❌ The function is not placed correctly.")
        show_file_structure()
        sys.exit(1)
"""
Hybrid Canvas Downloader Setup Guide
====================================

This guide shows you exactly how to implement the hybrid approach that actually
downloads files (like your friend's solution) into your existing Canvas downloader.

WHAT THIS SOLVES:
✅ Empty module downloads (your current problem)
✅ Missing PDFs and files that show in Canvas web interface
✅ API limitations that miss actual content

WHAT YOU GET:
🎯 Module structure from Canvas API (organization)
🎯 Actual files from web scraping (content)
🎯 Complete downloads with real PDFs and documents
"""

import os
import shutil
from pathlib import Path


def setup_hybrid_downloader():
    """Complete setup guide for hybrid downloader."""

    print("🚀 Hybrid Canvas Downloader Setup")
    print("=" * 50)

    # Step 1: File placement
    print("\n📁 STEP 1: File Placement")
    print("-" * 30)

    files_to_create = [
        {
            'source': 'Web Content Extractor',
            'destination': 'src/utils/web_content_extractor.py',
            'description': 'Core web scraping module'
        },
        {
            'source': 'Hybrid Modules Downloader',
            'destination': 'src/downloaders/hybrid_modules.py',
            'description': 'Enhanced modules downloader'
        }
    ]

    for file_info in files_to_create:
        print(f"✅ {file_info['destination']}")
        print(f"   Purpose: {file_info['description']}")

    # Step 2: Dependencies
    print("\n📦 STEP 2: Install Dependencies")
    print("-" * 30)
    print("Run this command:")
    print("pip install beautifulsoup4 requests")
    print()
    print("Dependencies needed:")
    print("✅ beautifulsoup4 - For HTML parsing")
    print("✅ requests - For web requests (already installed)")

    # Step 3: Cookies setup
    print("\n🍪 STEP 3: Browser Cookies Setup")
    print("-" * 30)
    print("1. Install browser extension: 'Get cookies.txt LOCALLY'")
    print("2. Go to your Canvas site (e.g., utec.instructure.com)")
    print("3. Log in to Canvas")
    print("4. Click the extension and export cookies")
    print("5. Save the file as: config/cookies.txt")
    print()
    print("📍 EXACT LOCATION: config/cookies.txt")
    print()
    print("File structure should look like:")
    print("project_root/")
    print("├── config/")
    print("│   └── cookies.txt  ← Your browser cookies here")
    print("├── src/")
    print("│   ├── utils/")
    print("│   │   └── web_content_extractor.py")
    print("│   └── downloaders/")
    print("│       └── hybrid_modules.py")
    print("└── ...")

    # Step 4: Integration
    print("\n🔧 STEP 4: Integration with Existing Code")
    print("-" * 30)
    print("Replace your existing modules downloader:")
    print()
    print("In src/downloaders/__init__.py, change:")
    print("❌ from .modules import ModulesDownloader")
    print("✅ from .hybrid_modules import HybridModulesDownloader as ModulesDownloader")
    print()
    print("Or in your orchestrator/main code:")
    print("❌ from src.downloaders.modules import ModulesDownloader")
    print("✅ from src.downloaders.hybrid_modules import HybridModulesDownloader")

    # Step 5: Testing
    print("\n🧪 STEP 5: Testing")
    print("-" * 30)
    print("Test with your problematic course:")
    print("1. Run your Canvas downloader")
    print("2. Select the Arte y Tecnología course")
    print("3. Check the results:")
    print()
    print("EXPECTED RESULTS:")
    print("✅ Module structure preserved (from API)")
    print("✅ Actual PDFs downloaded (from web scraping)")
    print("✅ Files folder with real content")
    print("✅ _HYBRID_summary.txt files showing success")
    print()
    print("FOLDER STRUCTURE AFTER SUCCESS:")
    print("downloads/")
    print("└── Arte y Tecnología (HH3101) - Teoría 1 - 2025 - 1/")
    print("    └── modules/")
    print("        ├── module_001_Sílabo y anexo/")
    print("        │   ├── files/")
    print("        │   │   ├── EL ORIGEN DEL ARTE.pdf")
    print("        │   │   └── Arte S.XIX. 1.pdf")
    print("        │   ├── items/")
    print("        │   └── Sílabo y anexo_HYBRID_summary.txt")
    print("        └── module_003_Semana 1/")
    print("            ├── files/")
    print("            │   └── introduccion-al-arte-occidental-del-siglo-xix.pdf")
    print("            └── ...")


def create_integration_patch():
    """Create a patch file to integrate with existing downloader."""

    patch_content = '''"""
Integration Patch for Existing Canvas Downloader
===============================================

Apply this patch to integrate the hybrid downloader with your existing code.
"""

# PATCH 1: Update downloaders/__init__.py
# =======================================

# FIND THIS LINE:
# from .modules import ModulesDownloader

# REPLACE WITH:
from .hybrid_modules import HybridModulesDownloader as ModulesDownloader

# OR ADD THIS IF THE IMPORT DOESN'T EXIST:
from .hybrid_modules import HybridModulesDownloader


# PATCH 2: Update your main download script (if needed)
# =====================================================

# IF you have direct imports like:
# from src.downloaders.modules import ModulesDownloader

# REPLACE WITH:
from src.downloaders.hybrid_modules import HybridModulesDownloader


# PATCH 3: Configuration (if needed)
# ==================================

# In config/config.json, ensure modules are enabled:
{
  "content_types": {
    "modules": {
      "enabled": true,
      "priority": 1,
      "download_module_content": true,
      "download_module_items": true,
      "download_associated_files": true,
      "create_module_index": true
    }
  }
}


# PATCH 4: Optional - Enhanced logging
# ===================================

# To see more details about the hybrid process, in your logging config:
{
  "logging": {
    "level": "INFO"  # or "DEBUG" for very detailed logs
  }
}
'''

    # Save patch file
    patch_file = Path("hybrid_integration.patch")
    with open(patch_file, 'w', encoding='utf-8') as f:
        f.write(patch_content)

    print(f"📝 Created integration patch: {patch_file}")
    return patch_file


def verify_setup():
    """Verify that the setup is correct."""

    print("\n🔍 STEP 6: Verification Checklist")
    print("-" * 30)

    checks = [
        {
            'name': 'Web Content Extractor',
            'path': 'src/utils/web_content_extractor.py',
            'required': True
        },
        {
            'name': 'Hybrid Modules Downloader',
            'path': 'src/downloaders/hybrid_modules.py',
            'required': True
        },
        {
            'name': 'Browser Cookies',
            'path': 'config/cookies.txt',
            'required': True
        },
        {
            'name': 'Config Directory',
            'path': 'config/',
            'required': True
        }
    ]

    all_good = True

    for check in checks:
        path = Path(check['path'])
        exists = path.exists()

        status = "✅" if exists else "❌"
        print(f"{status} {check['name']}: {check['path']}")

        if not exists and check['required']:
            all_good = False
            if check['path'].endswith('.txt'):
                print(f"   → Export browser cookies to this location")
            else:
                print(f"   → Create this file with the provided code")

    print()
    if all_good:
        print("🎉 Setup verification PASSED!")
        print("Your hybrid downloader is ready to use.")
    else:
        print("⚠️  Setup verification FAILED!")
        print("Please fix the missing files above.")

    return all_good


def create_test_script():
    """Create a test script to verify the hybrid downloader works."""

    test_script = '''#!/usr/bin/env python3
"""
Hybrid Downloader Test Script
============================

This script tests the hybrid downloader to make sure it works correctly.
"""

import sys
import os
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def test_imports():
    """Test that all required modules can be imported."""
    print("🧪 Testing imports...")

    try:
        from src.utils.web_content_extractor import create_web_content_extractor
        print("✅ Web content extractor import successful")
    except ImportError as e:
        print(f"❌ Web content extractor import failed: {e}")
        return False

    try:
        from src.downloaders.hybrid_modules import HybridModulesDownloader
        print("✅ Hybrid modules downloader import successful")
    except ImportError as e:
        print(f"❌ Hybrid modules downloader import failed: {e}")
        return False

    return True

def test_cookies():
    """Test that cookies file exists and is readable."""
    print("\\n🍪 Testing cookies...")

    cookies_path = Path("config/cookies.txt")

    if not cookies_path.exists():
        print(f"❌ Cookies file not found: {cookies_path}")
        print("   → Export browser cookies from Canvas to this location")
        return False

    try:
        with open(cookies_path, 'r') as f:
            content = f.read()
            if len(content.strip()) == 0:
                print("❌ Cookies file is empty")
                return False

            lines = [line for line in content.split('\\n') if line.strip() and not line.startswith('#')]
            print(f"✅ Cookies file loaded: {len(lines)} cookie entries")
            return True

    except Exception as e:
        print(f"❌ Error reading cookies file: {e}")
        return False

def test_web_extractor():
    """Test the web content extractor."""
    print("\\n🌐 Testing web content extractor...")

    try:
        from src.utils.web_content_extractor import create_web_content_extractor

        extractor = create_web_content_extractor()
        print("✅ Web content extractor created successfully")

        # Test if it can detect Canvas URL
        if hasattr(extractor, 'base_url') and extractor.base_url:
            print(f"✅ Detected Canvas URL: {extractor.base_url}")

        return True

    except Exception as e:
        print(f"❌ Web content extractor test failed: {e}")
        return False

def main():
    """Run all tests."""
    print("🚀 Hybrid Canvas Downloader Test Suite")
    print("=" * 50)

    tests = [
        ("Import Test", test_imports),
        ("Cookies Test", test_cookies), 
        ("Web Extractor Test", test_web_extractor)
    ]

    passed = 0
    total = len(tests)

    for test_name, test_func in tests:
        try:
            if test_func():
                passed += 1
        except Exception as e:
            print(f"❌ {test_name} crashed: {e}")

    print("\\n" + "=" * 50)
    print(f"📊 Test Results: {passed}/{total} passed")

    if passed == total:
        print("🎉 ALL TESTS PASSED!")
        print("Your hybrid downloader is ready to use.")
        print("\\n🚀 Next Steps:")
        print("1. Run your Canvas downloader")
        print("2. Select your problematic course")
        print("3. Watch it download actual files!")
    else:
        print("⚠️  SOME TESTS FAILED!")
        print("Please fix the issues above before using the hybrid downloader.")

    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
'''

    test_file = Path("test_hybrid_setup.py")
    with open(test_file, 'w', encoding='utf-8') as f:
        f.write(test_script)

    # Make it executable
    try:
        os.chmod(test_file, 0o755)
    except:
        pass  # Windows doesn't need this

    print(f"🧪 Created test script: {test_file}")
    print("Run with: python test_hybrid_setup.py")

    return test_file


def main():
    """Main setup function."""
    print("🛠️  HYBRID CANVAS DOWNLOADER SETUP")
    print("Solving the empty downloads problem with proven web scraping approach")
    print("=" * 70)

    # Run setup guide
    setup_hybrid_downloader()

    # Create integration patch
    patch_file = create_integration_patch()

    # Create test script
    test_file = create_test_script()

    # Verify setup
    setup_ok = verify_setup()

    print("\n" + "=" * 70)
    print("📋 SUMMARY")
    print("-" * 20)
    print("Files created:")
    print(f"✅ {patch_file} - Integration instructions")
    print(f"✅ {test_file} - Test script")
    print()
    print("What you need to do:")
    print("1. Save the web content extractor code to: src/utils/web_content_extractor.py")
    print("2. Save the hybrid downloader code to: src/downloaders/hybrid_modules.py")
    print("3. Export browser cookies to: config/cookies.txt")
    print("4. Install dependencies: pip install beautifulsoup4")
    print("5. Apply integration patch (see hybrid_integration.patch)")
    print("6. Run test: python test_hybrid_setup.py")
    print("7. Test with your problematic course")
    print()
    if setup_ok:
        print("🎉 Setup looks good! Ready to test.")
    else:
        print("⚠️  Complete the missing files above first.")

    print("\n💡 EXPECTED RESULT:")
    print("Your Arte y Tecnología course will download WITH actual PDF files!")


if __name__ == "__main__":
    main()
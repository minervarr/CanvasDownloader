"""
Canvas Downloader Setup Configuration

This file configures the package for installation and distribution.
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="canvas-downloader",
    version="0.1.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="A modular Canvas LMS content downloader with parallel processing",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/canvas-downloader",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Education",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "canvas-downloader=src.ui.cli:main",
        ],
    },
)

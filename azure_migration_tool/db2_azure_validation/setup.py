"""
Setup script for db2_azure_validation module.

Install with: pip install -e .
"""

from setuptools import setup, find_packages
import os

# Read README for long description
readme_path = os.path.join(os.path.dirname(__file__), "README.md")
long_description = ""
if os.path.exists(readme_path):
    with open(readme_path, "r", encoding="utf-8") as f:
        long_description = f.read()

# Read requirements
requirements_path = os.path.join(os.path.dirname(__file__), "requirements.txt")
requirements = []
if os.path.exists(requirements_path):
    with open(requirements_path, "r", encoding="utf-8") as f:
        requirements = [
            line.strip()
            for line in f
            if line.strip() and not line.startswith("#")
        ]

setup(
    name="db2_azure_validation",
    version="1.0.0",
    author="Migration Team",
    author_email="migration@example.com",
    description="DB2 to Azure SQL Migration Validation Module",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/your-org/db2-to-azure-migration-validation",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    python_requires=">=3.9",
    install_requires=requirements,
    extras_require={
        "db2": [
            "ibm-db>=3.1.0",
            "ibm-db-sa>=0.3.0",
        ],
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "black>=23.0.0",
            "mypy>=1.0.0",
        ],
    },
    package_data={
        "db2_azure_validation": [
            "*.json",
            "*.md",
            "*.txt",
        ],
    },
    include_package_data=True,
)

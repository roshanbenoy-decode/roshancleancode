# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project: Azure File Manager

A streamlined Python application for managing files in the `30-projects` Azure Blob Storage container with browser-based authentication.

## Commands

### Setup (using uv - recommended)
- Install uv: `pip install uv` (if not already installed)
- Create virtual environment: `uv venv superman` (or use existing `superman` venv)
- Activate virtual environment:
  - Windows: `superman\Scripts\activate`
  - Unix/MacOS: `source superman/bin/activate`
- Install dependencies: `uv sync` or `uv pip install -e .`
- Configure environment: Copy `.env.example` to `.env` and set your storage account name
- Ensure the `30-projects` container exists in your Azure Storage account

### Setup (traditional pip)
- Install dependencies: `pip install -r requirements.txt`
- Configure environment: Copy `.env.example` to `.env` and set your storage account name
- Ensure the `30-projects` container exists in your Azure Storage account

### Development
- Run main application: `python azure_file_manager.py`
- Run DataFrame creator: `python dfinsidefolder.py`

### Testing
- Manual testing through the CLI menu interfaces

## Architecture

This is a simple CLI application that uses Azure SDK for Python to interact with a single fixed Azure Blob Storage container: `30-projects`.

### Design Philosophy

- **Single Container Focus**: All operations work exclusively with the `30-projects` container
- **Path-Based Navigation**: Users specify path prefixes to navigate and filter blobs
- **Simplified Workflow**: No need to select containers - just work with paths directly

### Key Components

1. **config.py** - Configuration management
   - Loads environment variables from `.env` file
   - Validates required Azure Storage account name
   - Provides account URL construction

2. **azure_file_manager.py** - Main application
   - `AzureFileManager` class handles all Azure operations
   - Fixed container: `CONTAINER_NAME = "30-projects"`
   - Browser-based authentication using `InteractiveBrowserCredential`
   - Operations:
     - `get_folders_and_files(current_path)` - Get folders and files at a specific path level
     - `download_blob(blob_name)` - Download from fixed container
     - `upload_blob(local_file_path, blob_name)` - Upload to fixed container
   - Simple 4-option menu-driven CLI interface

3. **dfinsidefolder.py** - DataFrame creator for folder analysis
   - `AzureFolderDataFrame` class creates pandas DataFrames from blob paths
   - Takes a path input (e.g., "report documentation/Diu materials/")
   - Generates DataFrame with columns:
     - Name: filename only
     - File Type: extension (xlsx, png, pdf, etc.)
     - Size (MB): file size in megabytes
     - Last Modified: timestamp
     - Full Path: complete blob path
   - Option to export DataFrame to CSV
   - Skips folder markers (0-byte files and paths ending with /)

### Code Structure

- Browser authentication opens automatically on first run
- Connection test verifies `30-projects` container exists during initialization
- All downloads go to `downloads/` directory (created automatically)
- Uses Azure SDK clients: `BlobServiceClient`, `ContainerClient`, `BlobClient`
- Error handling for common Azure exceptions (ResourceNotFoundError, AzureError)

### Usage Flow

1. **Browse Folders**: Interactive hierarchical folder navigation
   - User can optionally enter a starting path or start at root
   - Displays folders and files at current level
   - Navigate into folders by number
   - Commands: back ('b'), jump to path ('j'), download ('d'), upload ('u'), quit ('q')
   - Only actual files (with size) are shown, preventing duplicate folder entries

2. **Download**: User enters full blob path (e.g., `project1/docs/file.txt`)
   - Downloads to `downloads/` directory with original filename

3. **Upload**: User enters local file path and optional destination path
   - If no destination path provided, uses original filename at container root

### Authentication Flow

1. `InteractiveBrowserCredential` triggers browser login
2. User authenticates with Microsoft/Azure account
3. Token stored by Azure SDK for subsequent requests
4. Requires Storage Blob Data Contributor role or similar permissions on `30-projects` container

## Code Reuse and Development Guidelines

**CRITICAL**: Before creating any new script or program, Claude MUST first explore the existing codebase to identify reusable components, patterns, and utilities. This ensures consistency, reduces code duplication, and maintains the project's architectural integrity.

### Development Process

1. **Explore First**: Always search the codebase for:
   - Existing classes that can be inherited or extended
   - Utility functions that can be reused
   - Configuration patterns already in use
   - Similar scripts that solve related problems

2. **Identify Reusable Components**: Check these key files before writing new code:
   - **azure_file_manager.py** - Base class with Azure authentication and blob operations
   - **config.py** - Environment variable and configuration management
   - **datafeed_scanner.py** - Example of inheriting from AzureFileManager
   - **dfinsidefolder.py** - DataFrame creation patterns for blob analysis
   - **check_column_consistency.py** - Data processing and HTML report generation
   - **check_table_consistency.py** - Master path definitions and consistency checking logic

3. **Extend, Don't Duplicate**: Prefer inheritance and composition over rewriting code

### Reusable Components Reference

#### AzureFileManager Class (azure_file_manager.py)
The foundation for any Azure blob storage operations. **Always inherit from this class** when creating new Azure-related scripts.

**Key capabilities:**
- Browser-based authentication (`InteractiveBrowserCredential`)
- Blob service client initialization
- Container client access
- Download/upload blob operations
- Connection testing

**Example usage:**
```python
from azure_file_manager import AzureFileManager

class MyNewScript(AzureFileManager):
    """Inherit authentication and blob operations."""

    def __init__(self):
        super().__init__()  # Handles all Azure authentication

    def my_custom_operation(self):
        # Use self.blob_service_client, self.CONTAINER_NAME, etc.
        container_client = self.blob_service_client.get_container_client(self.CONTAINER_NAME)
        # Your custom logic here
```

**Real example from codebase:**
- `DatafeedScanner` (datafeed_scanner.py) inherits from `AzureFileManager` to reuse authentication and blob operations

#### Configuration (config.py)
Centralized configuration management. **Always use this** for environment variables.

**Usage:**
```python
from config import Config

config = Config()
account_name = config.storage_account_name
```

#### Common Patterns to Reuse

1. **Output Location Prompts** (used in multiple scripts):
```python
output_dir = input("Enter output directory (press Enter for default 'downloads'): ").strip()
if not output_dir:
    output_dir = 'downloads'
os.makedirs(output_dir, exist_ok=True)
```

2. **Master Path Definitions** (consistency check scripts):
```python
MASTER_PATH_1 = "0000_test_parquet/100007-16_Showcase/Report Documentation/Datafeed"
MASTER_PATH_2 = "999999_WeitereKDdec/128019_18_Ruegenwalder_Welle4/Report Documentation/Datafeed"
```

3. **Temporary File Handling** (datafeed_scanner.py):
```python
temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
# Use temp file
# Clean up with os.unlink()
```

4. **DataFrame Export with User Prompt**:
- See datafeed_scanner.py `export_to_csv()` method
- See dfinsidefolder.py export functionality

### Best Practices

1. **Check before you create**: Use Glob/Grep to search for existing implementations
2. **Follow established patterns**: Match the coding style, error handling, and output formats of existing scripts
3. **Share common constants**: If multiple scripts use the same paths or configurations, consider where they should be defined
4. **Document reusable components**: When creating new utilities, document them for future reuse
5. **Ask the user**: If unsure whether to reuse or create new, ask the user for clarification

### Examples of Good Code Reuse

**Good Example:**
```python
# Creating a new blob analysis script
from azure_file_manager import AzureFileManager
import pandas as pd

class NewBlobAnalyzer(AzureFileManager):
    """Inherits Azure operations from base class."""
    def __init__(self):
        super().__init__()  # Reuses authentication
```

**Bad Example:**
```python
# DON'T recreate authentication from scratch
from azure.identity import InteractiveBrowserCredential
from azure.storage.blob import BlobServiceClient

# This duplicates code that already exists in AzureFileManager
credential = InteractiveBrowserCredential()
blob_service_client = BlobServiceClient(account_url=..., credential=credential)
```

## Output File Guidelines

**IMPORTANT**: When creating scripts that generate output files (CSV, HTML, Excel, JSON, etc.), Claude should ALWAYS follow these guidelines:

### Default Output Location
- **Default directory**: `downloads/` folder
- All generated output files should be saved to the `downloads/` directory by default
- The downloads folder is already created and used by the Azure File Manager application

### User Prompts for Output Location
- **Always ask the user** where they want to save output files
- Provide a clear prompt with the default option
- Example prompt format:
  ```python
  output_dir = input("Enter output directory (press Enter for default 'downloads'): ").strip()
  if not output_dir:
      output_dir = 'downloads'
  ```

### Best Practices
1. Use `os.path.join()` for cross-platform compatible file paths
2. Create the output directory if it doesn't exist using `os.makedirs(output_dir, exist_ok=True)`
3. Always import `os` module when handling file paths and directories
4. Display the full output path to the user after they make their selection
5. Use descriptive filenames with timestamps when appropriate (e.g., `report_20251016_101700.html`)

### Example Implementation
```python
import os
from datetime import datetime

# Ask user for output location
output_dir = input("Enter output directory (press Enter for default 'downloads'): ").strip()
if not output_dir:
    output_dir = 'downloads'

# Create directory if needed
os.makedirs(output_dir, exist_ok=True)

# Build file path
output_file = os.path.join(output_dir, 'my_report.html')
print(f"Output will be saved to: {output_file}")
```

This ensures consistency across all scripts and gives users control over where their files are saved while maintaining a sensible default.

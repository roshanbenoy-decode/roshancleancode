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

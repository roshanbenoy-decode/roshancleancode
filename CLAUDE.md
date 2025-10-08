# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project: Azure File Manager

A simple Python application for managing Azure Blob Storage with browser-based authentication.

## Commands

### Setup
- Install dependencies: `pip install -r requirements.txt`
- Configure environment: Copy `.env.example` to `.env` and set your storage account name

### Development
- Run application: `python azure_file_manager.py`

### Testing
- Manual testing through the CLI menu interface

## Architecture

This is a simple CLI application that uses Azure SDK for Python to interact with Azure Blob Storage.

### Key Components

1. **config.py** - Configuration management
   - Loads environment variables from `.env` file
   - Validates required Azure Storage account name
   - Provides account URL construction

2. **azure_file_manager.py** - Main application
   - `AzureFileManager` class handles all Azure operations
   - Browser-based authentication using `InteractiveBrowserCredential`
   - Operations: list containers/blobs, download, upload
   - Simple menu-driven CLI interface

### Code Structure

- Browser authentication opens automatically on first run
- All downloads go to `downloads/` directory (created automatically)
- Uses Azure SDK clients: `BlobServiceClient`, `ContainerClient`, `BlobClient`
- Error handling for common Azure exceptions (ResourceNotFoundError, AzureError)

### Authentication Flow

1. `InteractiveBrowserCredential` triggers browser login
2. User authenticates with Microsoft/Azure account
3. Token stored by Azure SDK for subsequent requests
4. Requires Storage Blob Data Contributor role or similar permissions

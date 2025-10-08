# Azure File Manager - Project Plan

## Overview
This is a simple Python application that allows you to interact with Azure Blob Storage using browser-based authentication. You can list, download, and upload files to Azure Storage containers.

## How It Works

### Authentication Flow
1. When you run the application, it uses `InteractiveBrowserCredential` from Azure Identity SDK
2. Your default web browser opens automatically
3. You sign in with your Microsoft/Azure account
4. The application receives authentication tokens
5. You can now perform file operations on Azure Storage

### Core Functionality

#### 1. List Files (Blobs)
- Connects to your Azure Storage account
- Lists all containers in your storage account
- Shows all blobs (files) within a selected container
- Displays blob names, sizes, and last modified dates

#### 2. Download Files
- Select a container and blob name
- Downloads the blob to a local `downloads/` directory
- Preserves the original filename
- Shows download progress

#### 3. Upload Files
- Select a local file from your computer
- Choose the destination container
- Uploads the file to Azure Blob Storage
- Option to overwrite existing files

## Project Structure

```
azure-file-manager/
│
├── projectplan.md              # This file - explains the project
├── requirements.txt            # Python package dependencies
├── .env.example               # Template for environment variables
├── config.py                  # Loads configuration from .env file
├── azure_file_manager.py      # Main application script
└── downloads/                 # Directory for downloaded files (created automatically)
```

## Technical Architecture

### Components

1. **Azure Identity** (`azure-identity` package)
   - Handles browser-based authentication
   - `InteractiveBrowserCredential` opens browser for login
   - Manages authentication tokens automatically

2. **Azure Storage Blob** (`azure-storage-blob` package)
   - `BlobServiceClient` - Main client for storage operations
   - `ContainerClient` - Manages containers
   - `BlobClient` - Handles individual blob operations

3. **Configuration** (`python-dotenv` package)
   - Loads settings from `.env` file
   - Stores Azure Storage account name
   - Optional: tenant ID for specific Azure AD tenants

### Data Flow

```
User → CLI Menu → azure_file_manager.py
                        ↓
                  InteractiveBrowserCredential
                        ↓
                  (Opens Browser)
                        ↓
                  User logs in to Microsoft
                        ↓
                  Token returned to app
                        ↓
                  BlobServiceClient
                        ↓
                  Azure Blob Storage
                        ↓
                  (List/Download/Upload)
                        ↓
                  Results shown to user
```

## Setup Instructions

### Prerequisites
- Python 3.8 or higher
- Azure subscription with a Storage Account
- Appropriate permissions (Storage Blob Data Contributor role)

### Step 1: Install Dependencies
```bash
pip install -r requirements.txt
```

### Step 2: Configure Environment
1. Copy `.env.example` to `.env`
2. Edit `.env` and add your Azure Storage account name:
   ```
   AZURE_STORAGE_ACCOUNT_NAME=yourstorageaccountname
   ```
3. Optional: Add tenant ID if using specific Azure AD tenant

### Step 3: Run the Application
```bash
python azure_file_manager.py
```

### Step 4: First-Time Authentication
- Browser window opens automatically
- Sign in with your Microsoft/Azure credentials
- Grant permissions if prompted
- Return to terminal to continue

## Usage Examples

### Example 1: List All Files in a Container
1. Run the application
2. Select option "1. List containers and blobs"
3. Choose a container from the list
4. View all files in that container

### Example 2: Download a File
1. Run the application
2. Select option "2. Download a blob"
3. Enter container name (e.g., "documents")
4. Enter blob name (e.g., "report.pdf")
5. File downloads to `downloads/report.pdf`

### Example 3: Upload a File
1. Run the application
2. Select option "3. Upload a blob"
3. Enter local file path (e.g., "C:\\Users\\John\\file.txt")
4. Enter container name (e.g., "backups")
5. Enter desired blob name (or keep original filename)
6. File uploads to Azure Storage

## Security Considerations

### Authentication
- Uses interactive browser login (OAuth 2.0)
- No passwords stored in code or files
- Tokens managed securely by Azure SDK
- Recommended for development and testing

### Permissions Required
Your Azure account needs one of these roles:
- **Storage Blob Data Contributor** - Can read, write, and delete blobs
- **Storage Blob Data Reader** - Can only read blobs (for download/list only)
- **Storage Blob Data Owner** - Full control over blobs

### Best Practices
1. Don't commit `.env` file to version control
2. Use `.env.example` as a template only
3. For production, consider using Managed Identity or Service Principal
4. Browser credential is best for local development

## Limitations and Considerations

### Current Implementation
- Works with Blob Storage only (not Files, Queues, or Tables)
- Browser authentication requires user interaction
- Single storage account at a time
- Downloads to fixed `downloads/` directory

### Future Enhancements (Not Implemented)
- Support for Azure File Shares
- Batch download/upload operations
- Progress bars for large files
- Delete blob functionality
- Container creation/deletion
- Custom download directory selection

## Troubleshooting

### Issue: Browser doesn't open
**Solution**: Check firewall settings or manually copy authentication URL

### Issue: Authentication fails
**Solution**: Ensure you have correct Azure permissions (Storage Blob Data Contributor)

### Issue: Storage account not found
**Solution**: Verify storage account name in `.env` file is correct

### Issue: Cannot download/upload
**Solution**: Check that container exists and you have appropriate role assignment

## Dependencies

### Required Packages
```
azure-identity==1.19.0         # Authentication with browser
azure-storage-blob==12.24.0    # Blob storage operations
python-dotenv==1.0.1           # Environment variable management
```

### Why These Packages?
- **azure-identity**: Provides `InteractiveBrowserCredential` for easy OAuth login
- **azure-storage-blob**: Official SDK for Azure Blob Storage operations
- **python-dotenv**: Simple configuration management without hardcoding values

## Additional Resources

- [Azure Storage Python SDK Documentation](https://learn.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python)
- [Azure Identity Documentation](https://learn.microsoft.com/en-us/python/api/overview/azure/identity-readme)
- [Azure Blob Storage Concepts](https://learn.microsoft.com/en-us/azure/storage/blobs/storage-blobs-introduction)

## Support

For issues related to:
- **Azure SDK**: Check official Microsoft documentation
- **This project**: Review this plan and code comments
- **Azure permissions**: Contact your Azure administrator

"""
Azure File Manager - Simple Azure Blob Storage operations with browser authentication.

Features:
- List blobs by path in the fixed '30-projects' container
- Download blobs from Azure Storage
- Upload files to Azure Storage
- Browser-based authentication (InteractiveBrowserCredential)
"""

import os
import sys
from pathlib import Path
from azure.identity import InteractiveBrowserCredential
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError, AzureError
from config import get_config


class AzureFileManager:
    """Manages Azure Blob Storage operations with browser authentication."""

    CONTAINER_NAME = "30-projects"  # Fixed container name

    def __init__(self, credential=None):
        """Initialize the Azure File Manager with browser authentication or provided credential.

        Args:
            credential: Optional Azure credential (for web app use). If not provided,
                       uses InteractiveBrowserCredential (for CLI use).
        """
        try:
            self.config = get_config()

            # If credential is provided (web app), use it directly
            if credential:
                print("Using provided user credential for authentication.")
                self.credential = credential
            else:
                # CLI mode: Use browser authentication
                print(f"Configuration loaded: {self.config}")
                print("\nAuthenticating with Azure...")
                print("Your browser will open for authentication. Please sign in.")

                # Create credential with browser authentication
                credential_kwargs = {}
                if self.config.tenant_id:
                    credential_kwargs['tenant_id'] = self.config.tenant_id

                self.credential = InteractiveBrowserCredential(**credential_kwargs)

            # Create BlobServiceClient
            self.blob_service_client = BlobServiceClient(
                account_url=self.config.account_url,
                credential=self.credential
            )

            # Test connection by verifying the container exists
            print("Testing connection...")
            container_client = self.blob_service_client.get_container_client(self.CONTAINER_NAME)
            container_client.get_container_properties()
            print(f"✓ Successfully authenticated and connected to container '{self.CONTAINER_NAME}'!\n")

        except ValueError as e:
            print(f"Configuration error: {e}")
            sys.exit(1)
        except ResourceNotFoundError:
            print(f"Container '{self.CONTAINER_NAME}' not found.")
            print("Please ensure the '30-projects' container exists in your storage account.")
            sys.exit(1)
        except AzureError as e:
            print(f"Azure authentication error: {e}")
            print("\nTroubleshooting:")
            print("1. Ensure you have the correct permissions (Storage Blob Data Contributor)")
            print("2. Verify your storage account name in .env file")
            print("3. Check if your Azure account has access to the storage account")
            print(f"4. Ensure the '{self.CONTAINER_NAME}' container exists")
            sys.exit(1)
        except Exception as e:
            print(f"Unexpected error: {e}")
            sys.exit(1)

    def list_blobs_by_path(self, path_prefix=""):
        """List all blobs in the fixed container with an optional path prefix."""
        try:
            if path_prefix:
                print(f"\nFetching blobs from container '{self.CONTAINER_NAME}' with path '{path_prefix}'...\n")
            else:
                print(f"\nFetching all blobs from container '{self.CONTAINER_NAME}'...\n")

            container_client = self.blob_service_client.get_container_client(self.CONTAINER_NAME)
            blobs = container_client.list_blobs(name_starts_with=path_prefix)
            blob_list = []

            for blob in blobs:
                size_mb = blob.size / (1024 * 1024)
                print(f"  - {blob.name} ({size_mb:.2f} MB) [Modified: {blob.last_modified}]")
                blob_list.append(blob.name)

            if not blob_list:
                if path_prefix:
                    print(f"  No blobs found with path '{path_prefix}'.")
                else:
                    print("  No blobs found in this container.")

            return blob_list

        except AzureError as e:
            print(f"Error listing blobs: {e}")
            return []

    def get_folders_and_files(self, current_path=""):
        """Get folders and files at the current path level.

        Returns a tuple: (folders, files)
        - folders: list of folder names at current level
        - files: list of file names at current level
        """
        try:
            container_client = self.blob_service_client.get_container_client(self.CONTAINER_NAME)
            blobs = container_client.list_blobs(name_starts_with=current_path)

            folders = set()
            files = []

            # Ensure current_path ends with / if not empty
            if current_path and not current_path.endswith('/'):
                current_path += '/'

            for blob in blobs:
                # Get the relative path from current_path
                relative_path = blob.name[len(current_path):]

                # Skip empty relative paths
                if not relative_path:
                    continue

                # Check if this blob is in a subfolder
                if '/' in relative_path:
                    # Extract the folder name (first part before /)
                    folder_name = relative_path.split('/')[0]
                    # Only add non-empty folder names
                    if folder_name:
                        folders.add(folder_name)
                else:
                    # This is a file at the current level (no slash means it's a file)
                    # Only add actual files: must have size > 0 and not end with /
                    if blob.size > 0 and not relative_path.endswith('/'):
                        files.append({
                            'name': relative_path,
                            'full_path': blob.name,
                            'size': blob.size,
                            'modified': blob.last_modified
                        })

            return sorted(list(folders)), files

        except AzureError as e:
            print(f"Error listing contents: {e}")
            return [], []

    def download_blob(self, blob_name, download_path=None):
        """Download a blob from the fixed container."""
        try:
            # Create downloads directory if it doesn't exist
            downloads_dir = Path("downloads")
            downloads_dir.mkdir(exist_ok=True)

            # Set download path
            if download_path is None:
                download_path = downloads_dir / Path(blob_name).name
            else:
                download_path = Path(download_path)

            print(f"\nDownloading '{blob_name}' from container '{self.CONTAINER_NAME}'...")

            # Get blob client and download
            blob_client = self.blob_service_client.get_blob_client(
                container=self.CONTAINER_NAME,
                blob=blob_name
            )

            with open(download_path, "wb") as file:
                download_stream = blob_client.download_blob()
                file.write(download_stream.readall())

            print(f"✓ Successfully downloaded to: {download_path.absolute()}")
            return True

        except ResourceNotFoundError:
            print(f"Blob '{blob_name}' not found in container '{self.CONTAINER_NAME}'.")
            return False
        except AzureError as e:
            print(f"Error downloading blob: {e}")
            return False
        except Exception as e:
            print(f"Unexpected error during download: {e}")
            return False

    def upload_blob(self, local_file_path, blob_name=None, overwrite=True):
        """Upload a file to the fixed container."""
        try:
            local_path = Path(local_file_path)

            if not local_path.exists():
                print(f"File not found: {local_file_path}")
                return False

            if not local_path.is_file():
                print(f"Path is not a file: {local_file_path}")
                return False

            # Use original filename if blob_name not provided
            if blob_name is None:
                blob_name = local_path.name

            print(f"\nUploading '{local_path.name}' to container '{self.CONTAINER_NAME}' as '{blob_name}'...")

            # Get container client
            container_client = self.blob_service_client.get_container_client(self.CONTAINER_NAME)

            # Upload file
            with open(local_path, "rb") as data:
                container_client.upload_blob(
                    name=blob_name,
                    data=data,
                    overwrite=overwrite
                )

            print(f"✓ Successfully uploaded to: {self.CONTAINER_NAME}/{blob_name}")
            return True

        except AzureError as e:
            print(f"Error uploading blob: {e}")
            return False
        except Exception as e:
            print(f"Unexpected error during upload: {e}")
            return False


def print_menu():
    """Print the main menu."""
    print("\n" + "="*50)
    print("Azure File Manager - Container: 30-projects")
    print("="*50)
    print("1. Browse folders")
    print("2. Download a blob")
    print("3. Upload a blob")
    print("4. Exit")
    print("="*50)


def browse_folders(manager, start_path=""):
    """Interactive folder browser with hierarchical navigation."""
    current_path = start_path

    while True:
        # Display current location
        display_path = current_path if current_path else "/"
        print("\n" + "="*60)
        print(f"Current location: {display_path}")
        print("="*60)

        # Get folders and files at current level
        folders, files = manager.get_folders_and_files(current_path)

        # Display folders
        if folders:
            print("\n📁 FOLDERS:")
            for idx, folder in enumerate(folders, 1):
                print(f"  {idx}. {folder}/")
        else:
            print("\n📁 FOLDERS: (none)")

        # Display files
        if files:
            print("\n📄 FILES:")
            for idx, file_info in enumerate(files, len(folders) + 1):
                size_mb = file_info['size'] / (1024 * 1024)
                print(f"  {idx}. {file_info['name']} ({size_mb:.2f} MB)")
        else:
            print("\n📄 FILES: (none)")

        # Navigation menu
        print("\n" + "-"*60)
        print("Commands:")
        print("  [number] - Enter folder or select file")
        print("  'b' - Go back to parent folder")
        print("  'j' - Jump to specific path")
        print("  'd' - Download a file")
        print("  'u' - Upload a file to current location")
        print("  'q' - Return to main menu")
        print("-"*60)

        choice = input("\nEnter your choice: ").strip().lower()

        if choice == 'q':
            break
        elif choice == 'b':
            # Go back to parent folder
            if current_path:
                # Remove trailing slash if exists
                path = current_path.rstrip('/')
                # Go to parent
                if '/' in path:
                    current_path = path.rsplit('/', 1)[0] + '/'
                else:
                    current_path = ""
            else:
                print("Already at root level.")
        elif choice == 'j':
            # Jump to specific path
            jump_path = input("Enter path to jump to (or press Enter for root): ").strip()
            current_path = jump_path
        elif choice == 'd':
            # Download file
            if files:
                file_num = input("Enter file number to download: ").strip()
                try:
                    file_idx = int(file_num) - len(folders) - 1
                    if 0 <= file_idx < len(files):
                        manager.download_blob(files[file_idx]['full_path'])
                    else:
                        print("Invalid file number.")
                except ValueError:
                    print("Invalid input. Please enter a number.")
            else:
                print("No files in current location.")
        elif choice == 'u':
            # Upload file to current location
            local_file = input("Enter local file path: ").strip()
            if local_file:
                filename = Path(local_file).name
                # Construct blob path with current location
                if current_path:
                    blob_path = current_path.rstrip('/') + '/' + filename
                else:
                    blob_path = filename
                manager.upload_blob(local_file, blob_path)
            else:
                print("File path is required.")
        elif choice.isdigit():
            # Navigate to selected folder or display file info
            item_num = int(choice)
            total_items = len(folders) + len(files)

            if 1 <= item_num <= total_items:
                if item_num <= len(folders):
                    # Navigate to folder
                    selected_folder = folders[item_num - 1]
                    if current_path:
                        current_path = current_path.rstrip('/') + '/' + selected_folder + '/'
                    else:
                        current_path = selected_folder + '/'
                else:
                    # File selected
                    file_idx = item_num - len(folders) - 1
                    file_info = files[file_idx]
                    print(f"\n📄 File: {file_info['name']}")
                    print(f"   Full path: {file_info['full_path']}")
                    print(f"   Size: {file_info['size'] / (1024 * 1024):.2f} MB")
                    print(f"   Modified: {file_info['modified']}")
                    download = input("\nDownload this file? (y/n): ").strip().lower()
                    if download == 'y':
                        manager.download_blob(file_info['full_path'])
            else:
                print("Invalid number.")
        else:
            print("Invalid command.")


def main():
    """Main function to run the Azure File Manager CLI."""
    print("Welcome to Azure File Manager!\n")
    print("Working with container: 30-projects\n")

    # Initialize the file manager (this will trigger authentication)
    manager = AzureFileManager()

    while True:
        print_menu()
        choice = input("\nEnter your choice (1-4): ").strip()

        if choice == "1":
            # Browse folders with optional starting path
            start_path = input("\nEnter starting path (or press Enter for root): ").strip()
            browse_folders(manager, start_path)

        elif choice == "2":
            # Download a blob
            blob_name = input("\nEnter blob name/path: ").strip()

            if blob_name:
                manager.download_blob(blob_name)
            else:
                print("Blob name is required.")

        elif choice == "3":
            # Upload a blob
            local_file = input("\nEnter local file path: ").strip()
            blob_name = input("Enter destination blob name/path (press Enter to use original filename): ").strip()

            if local_file:
                blob_name = blob_name if blob_name else None
                manager.upload_blob(local_file, blob_name)
            else:
                print("Local file path is required.")

        elif choice == "4":
            print("\nThank you for using Azure File Manager. Goodbye!")
            break

        else:
            print("\nInvalid choice. Please enter 1-4.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user. Goodbye!")
        sys.exit(0)

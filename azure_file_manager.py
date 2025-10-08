"""
Azure File Manager - Simple Azure Blob Storage operations with browser authentication.

Features:
- List containers and blobs
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

    def __init__(self):
        """Initialize the Azure File Manager with browser authentication."""
        try:
            self.config = get_config()
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

            # Test connection by listing containers
            print("Testing connection...")
            next(self.blob_service_client.list_containers(), None)
            print("✓ Successfully authenticated!\n")

        except ValueError as e:
            print(f"Configuration error: {e}")
            sys.exit(1)
        except AzureError as e:
            print(f"Azure authentication error: {e}")
            print("\nTroubleshooting:")
            print("1. Ensure you have the correct permissions (Storage Blob Data Contributor)")
            print("2. Verify your storage account name in .env file")
            print("3. Check if your Azure account has access to the storage account")
            sys.exit(1)
        except Exception as e:
            print(f"Unexpected error: {e}")
            sys.exit(1)

    def list_containers(self):
        """List all containers in the storage account."""
        try:
            print("Fetching containers...\n")
            containers = self.blob_service_client.list_containers()
            container_list = []

            for container in containers:
                print(f"Container: {container.name}")
                container_list.append(container.name)

            if not container_list:
                print("No containers found.")

            return container_list

        except AzureError as e:
            print(f"Error listing containers: {e}")
            return []

    def list_blobs(self, container_name):
        """List all blobs in a specific container."""
        try:
            print(f"\nFetching blobs from container '{container_name}'...\n")
            container_client = self.blob_service_client.get_container_client(container_name)
            blobs = container_client.list_blobs()
            blob_list = []

            for blob in blobs:
                size_mb = blob.size / (1024 * 1024)
                print(f"  - {blob.name} ({size_mb:.2f} MB) [Modified: {blob.last_modified}]")
                blob_list.append(blob.name)

            if not blob_list:
                print("  No blobs found in this container.")

            return blob_list

        except ResourceNotFoundError:
            print(f"Container '{container_name}' not found.")
            return []
        except AzureError as e:
            print(f"Error listing blobs: {e}")
            return []

    def download_blob(self, container_name, blob_name, download_path=None):
        """Download a blob from Azure Storage."""
        try:
            # Create downloads directory if it doesn't exist
            downloads_dir = Path("downloads")
            downloads_dir.mkdir(exist_ok=True)

            # Set download path
            if download_path is None:
                download_path = downloads_dir / blob_name
            else:
                download_path = Path(download_path)

            print(f"\nDownloading '{blob_name}' from container '{container_name}'...")

            # Get blob client and download
            blob_client = self.blob_service_client.get_blob_client(
                container=container_name,
                blob=blob_name
            )

            with open(download_path, "wb") as file:
                download_stream = blob_client.download_blob()
                file.write(download_stream.readall())

            print(f"✓ Successfully downloaded to: {download_path.absolute()}")
            return True

        except ResourceNotFoundError:
            print(f"Blob '{blob_name}' not found in container '{container_name}'.")
            return False
        except AzureError as e:
            print(f"Error downloading blob: {e}")
            return False
        except Exception as e:
            print(f"Unexpected error during download: {e}")
            return False

    def upload_blob(self, container_name, local_file_path, blob_name=None, overwrite=True):
        """Upload a file to Azure Blob Storage."""
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

            print(f"\nUploading '{local_path.name}' to container '{container_name}' as '{blob_name}'...")

            # Get container client
            container_client = self.blob_service_client.get_container_client(container_name)

            # Upload file
            with open(local_path, "rb") as data:
                container_client.upload_blob(
                    name=blob_name,
                    data=data,
                    overwrite=overwrite
                )

            print(f"✓ Successfully uploaded to: {container_name}/{blob_name}")
            return True

        except ResourceNotFoundError:
            print(f"Container '{container_name}' not found.")
            return False
        except AzureError as e:
            print(f"Error uploading blob: {e}")
            return False
        except Exception as e:
            print(f"Unexpected error during upload: {e}")
            return False


def print_menu():
    """Print the main menu."""
    print("\n" + "="*50)
    print("Azure File Manager")
    print("="*50)
    print("1. List containers and blobs")
    print("2. Download a blob")
    print("3. Upload a blob")
    print("4. Exit")
    print("="*50)


def main():
    """Main function to run the Azure File Manager CLI."""
    print("Welcome to Azure File Manager!\n")

    # Initialize the file manager (this will trigger authentication)
    manager = AzureFileManager()

    while True:
        print_menu()
        choice = input("\nEnter your choice (1-4): ").strip()

        if choice == "1":
            # List containers and blobs
            containers = manager.list_containers()
            if containers:
                print("\nWould you like to view blobs in a specific container? (y/n): ", end="")
                if input().strip().lower() == 'y':
                    container_name = input("Enter container name: ").strip()
                    if container_name:
                        manager.list_blobs(container_name)

        elif choice == "2":
            # Download a blob
            container_name = input("\nEnter container name: ").strip()
            blob_name = input("Enter blob name: ").strip()

            if container_name and blob_name:
                manager.download_blob(container_name, blob_name)
            else:
                print("Container name and blob name are required.")

        elif choice == "3":
            # Upload a blob
            local_file = input("\nEnter local file path: ").strip()
            container_name = input("Enter destination container name: ").strip()
            blob_name = input("Enter blob name (press Enter to use original filename): ").strip()

            if local_file and container_name:
                blob_name = blob_name if blob_name else None
                manager.upload_blob(container_name, local_file, blob_name)
            else:
                print("Local file path and container name are required.")

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

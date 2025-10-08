"""
DataFrame Inside Folder - Create a DataFrame of files in a specific Azure blob path.

This script connects to Azure Blob Storage and creates a pandas DataFrame
with file information (name, type, size, last modified) for a given path.
"""

import os
import sys
import pandas as pd
from pathlib import Path
from azure.identity import InteractiveBrowserCredential
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError, AzureError
from config import get_config


class AzureFolderDataFrame:
    """Creates DataFrame from Azure Blob Storage folder contents."""

    CONTAINER_NAME = "30-projects"

    def __init__(self):
        """Initialize with Azure authentication."""
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

            # Test connection
            print("Testing connection...")
            container_client = self.blob_service_client.get_container_client(self.CONTAINER_NAME)
            container_client.get_container_properties()
            print(f"✓ Successfully authenticated and connected to container '{self.CONTAINER_NAME}'!\n")

        except ValueError as e:
            print(f"Configuration error: {e}")
            sys.exit(1)
        except ResourceNotFoundError:
            print(f"Container '{self.CONTAINER_NAME}' not found.")
            sys.exit(1)
        except AzureError as e:
            print(f"Azure authentication error: {e}")
            sys.exit(1)
        except Exception as e:
            print(f"Unexpected error: {e}")
            sys.exit(1)

    def get_file_type(self, filename):
        """Extract file extension/type from filename."""
        ext = Path(filename).suffix.lower()
        if ext:
            return ext[1:]  # Remove the dot
        return "unknown"

    def create_dataframe(self, path_prefix=""):
        """Create a DataFrame with file information from the specified path."""
        try:
            print(f"\nFetching files from path: '{path_prefix}'...\n")

            container_client = self.blob_service_client.get_container_client(self.CONTAINER_NAME)
            blobs = container_client.list_blobs(name_starts_with=path_prefix)

            # Collect file data
            file_data = []

            for blob in blobs:
                # Skip folder markers (size 0 or ending with /)
                if blob.size == 0 or blob.name.endswith('/'):
                    continue

                # Extract just the filename from full path
                filename = Path(blob.name).name
                file_type = self.get_file_type(blob.name)
                size_mb = blob.size / (1024 * 1024)

                file_data.append({
                    'Name': filename,
                    'File Type': file_type,
                    'Size (MB)': round(size_mb, 2),
                    'Last Modified': blob.last_modified,
                    'Full Path': blob.name
                })

            if not file_data:
                print(f"No files found in path '{path_prefix}'")
                return pd.DataFrame()

            # Create DataFrame
            df = pd.DataFrame(file_data)

            # Sort by name
            df = df.sort_values('Name').reset_index(drop=True)

            print(f"✓ Found {len(df)} files\n")
            return df

        except AzureError as e:
            print(f"Error fetching files: {e}")
            return pd.DataFrame()


def main():
    """Main function."""
    print("=" * 60)
    print("Azure Blob Storage - DataFrame Creator")
    print("Container: 30-projects")
    print("=" * 60)
    print()

    # Initialize
    manager = AzureFolderDataFrame()

    # Get path from user
    path = input("Enter the path to analyze (e.g., 'report documentation/Diu materials/'): ").strip()

    # Create DataFrame
    df = manager.create_dataframe(path)

    if not df.empty:
        print("\n" + "=" * 60)
        print("FILE SUMMARY")
        print("=" * 60)
        print(df.to_string(index=False))
        print("\n" + "=" * 60)
        print(f"Total files: {len(df)}")
        print(f"Total size: {df['Size (MB)'].sum():.2f} MB")
        print("=" * 60)

        # Offer to save
        save = input("\nSave to CSV? (y/n): ").strip().lower()
        if save == 'y':
            output_file = input("Enter output filename (default: folder_contents.csv): ").strip()
            if not output_file:
                output_file = "folder_contents.csv"

            df.to_csv(output_file, index=False)
            print(f"✓ Saved to {output_file}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user. Goodbye!")
        sys.exit(0)

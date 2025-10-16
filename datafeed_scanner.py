"""
Datafeed Scanner - Scan Azure Blob Storage for Datafeed folders and extract metadata.

This script searches through the '30-projects' container for folders named 'Datafeed',
analyzes Excel files (DimManager.xlsx) and Parquet files within them, and exports
metadata (table names and column names) to a CSV report.
"""

import os
import sys
import tempfile
import time
from pathlib import Path
from datetime import datetime
import pandas as pd
from azure.identity import InteractiveBrowserCredential
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError, AzureError
from config import get_config


class DatafeedScanner:
    """Scans Azure Blob Storage for Datafeed folders and extracts metadata."""

    CONTAINER_NAME = "30-projects"
    EXCEL_FILENAME = "DimManager.xlsx"

    def __init__(self):
        """Initialize the Datafeed Scanner with browser authentication."""
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

    def scan_for_datafeed_folders(self):
        """Scan all blobs and identify Datafeed folders.

        Returns:
            list: List of unique Datafeed folder paths
        """
        try:
            print("Scanning container for 'Datafeed' folders...")
            print("-" * 80)

            container_client = self.blob_service_client.get_container_client(self.CONTAINER_NAME)
            blobs = container_client.list_blobs()

            datafeed_folders = set()

            for blob in blobs:
                # Check if path contains 'Datafeed' as a folder
                path_parts = blob.name.split('/')

                # Look for 'Datafeed' folder in the path
                for i, part in enumerate(path_parts):
                    if part.lower() == 'datafeed':
                        # Construct the path up to and including 'Datafeed'
                        datafeed_path = '/'.join(path_parts[:i+1]) + '/'
                        datafeed_folders.add(datafeed_path)
                        break

            sorted_folders = sorted(list(datafeed_folders))
            print(f"✓ Found {len(sorted_folders)} Datafeed folder(s)\n")

            if sorted_folders:
                print("Datafeed folders found:")
                for folder in sorted_folders:
                    print(f"  - {folder}")
                print()

            return sorted_folders

        except AzureError as e:
            print(f"Error scanning blobs: {e}")
            return []

    def get_files_in_datafeed(self, datafeed_path):
        """Get list of files in a specific Datafeed folder.

        Args:
            datafeed_path: Path to the Datafeed folder

        Returns:
            dict: Dictionary with 'excel' and 'parquet' file lists
        """
        try:
            container_client = self.blob_service_client.get_container_client(self.CONTAINER_NAME)
            blobs = container_client.list_blobs(name_starts_with=datafeed_path)

            files = {
                'excel': None,
                'parquet': []
            }

            for blob in blobs:
                # Skip folder markers
                if blob.size == 0 or blob.name.endswith('/'):
                    continue

                # Get filename
                filename = blob.name.split('/')[-1]

                # Check for DimManager.xlsx
                if filename.lower() == self.EXCEL_FILENAME.lower():
                    files['excel'] = blob.name

                # Check for parquet files
                elif filename.lower().endswith('.parquet'):
                    files['parquet'].append(blob.name)

            return files

        except AzureError as e:
            print(f"Error listing files in {datafeed_path}: {e}")
            return {'excel': None, 'parquet': []}

    def download_blob_to_temp(self, blob_name):
        """Download a blob to a temporary file.

        Args:
            blob_name: Full path to the blob

        Returns:
            str: Path to temporary file, or None if failed
        """
        try:
            # Create temporary file
            suffix = Path(blob_name).suffix
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
            temp_path = temp_file.name
            temp_file.close()

            # Download blob
            blob_client = self.blob_service_client.get_blob_client(
                container=self.CONTAINER_NAME,
                blob=blob_name
            )

            with open(temp_path, "wb") as file:
                download_stream = blob_client.download_blob()
                file.write(download_stream.readall())

            return temp_path

        except Exception as e:
            print(f"  ✗ Error downloading {blob_name}: {e}")
            return None

    def analyze_excel_file(self, excel_path, datafeed_path):
        """Analyze Excel file and extract sheet names and columns.

        Args:
            excel_path: Path to the downloaded Excel file
            datafeed_path: Path to the Datafeed folder (for reporting)

        Returns:
            list: List of dictionaries with metadata
        """
        results = []

        try:
            # Read Excel file
            excel_file = pd.ExcelFile(excel_path, engine='openpyxl')

            print(f"  ✓ Found {len(excel_file.sheet_names)} sheet(s) in DimManager.xlsx")

            for sheet_name in excel_file.sheet_names:
                try:
                    # Read sheet
                    df = pd.read_excel(excel_file, sheet_name=sheet_name)

                    # Get column names
                    columns = ','.join(df.columns.tolist())

                    results.append({
                        'Path': datafeed_path.rstrip('/'),
                        'Source_Type': 'Excel',
                        'Table_Name': sheet_name,
                        'Columns': columns
                    })

                except Exception as e:
                    print(f"  ✗ Error reading sheet '{sheet_name}': {e}")

            # Close the Excel file explicitly
            excel_file.close()

        except Exception as e:
            print(f"  ✗ Error analyzing Excel file: {e}")

        return results

    def analyze_parquet_file(self, parquet_blob_name, datafeed_path):
        """Analyze Parquet file and extract column names.

        Args:
            parquet_blob_name: Full blob path to the parquet file
            datafeed_path: Path to the Datafeed folder (for reporting)

        Returns:
            dict: Dictionary with metadata, or None if failed
        """
        try:
            # Download parquet file
            temp_path = self.download_blob_to_temp(parquet_blob_name)
            if not temp_path:
                return None

            try:
                # Read parquet file
                df = pd.read_parquet(temp_path, engine='pyarrow')

                # Get column names
                columns = ','.join(df.columns.tolist())

                # Get filename
                filename = parquet_blob_name.split('/')[-1]

                return {
                    'Path': datafeed_path.rstrip('/'),
                    'Source_Type': 'Parquet',
                    'Table_Name': filename,
                    'Columns': columns
                }

            finally:
                # Clean up temp file
                if os.path.exists(temp_path):
                    os.unlink(temp_path)

        except Exception as e:
            print(f"  ✗ Error analyzing parquet file {parquet_blob_name}: {e}")
            return None

    def generate_report(self):
        """Generate comprehensive report of all Datafeed folders.

        Returns:
            pd.DataFrame: DataFrame with all metadata
        """
        # Find all Datafeed folders
        datafeed_folders = self.scan_for_datafeed_folders()

        if not datafeed_folders:
            print("No Datafeed folders found in the container.")
            return pd.DataFrame()

        # Collect all metadata
        all_results = []

        print("Analyzing Datafeed folders...")
        print("=" * 80)

        for idx, datafeed_path in enumerate(datafeed_folders, 1):
            print(f"\n[{idx}/{len(datafeed_folders)}] Processing: {datafeed_path}")
            print("-" * 80)

            # Get files in this Datafeed folder
            files = self.get_files_in_datafeed(datafeed_path)

            # Analyze Excel file if exists
            if files['excel']:
                print(f"  Found DimManager.xlsx")
                temp_excel_path = self.download_blob_to_temp(files['excel'])

                if temp_excel_path:
                    try:
                        excel_results = self.analyze_excel_file(temp_excel_path, datafeed_path)
                        all_results.extend(excel_results)
                    finally:
                        # Clean up temp file - wait a bit for Windows to release file handle
                        if os.path.exists(temp_excel_path):
                            time.sleep(0.1)
                            try:
                                os.unlink(temp_excel_path)
                            except PermissionError:
                                # If still locked, try again after a longer wait
                                time.sleep(0.5)
                                try:
                                    os.unlink(temp_excel_path)
                                except PermissionError:
                                    print(f"  Warning: Could not delete temp file {temp_excel_path}")
            else:
                print(f"  ✗ DimManager.xlsx not found")

            # Analyze Parquet files
            if files['parquet']:
                print(f"  Found {len(files['parquet'])} parquet file(s)")

                for parquet_blob in files['parquet']:
                    filename = parquet_blob.split('/')[-1]
                    print(f"    Analyzing: {filename}")

                    result = self.analyze_parquet_file(parquet_blob, datafeed_path)
                    if result:
                        all_results.append(result)
                        print(f"      ✓ Extracted {len(result['Columns'].split(','))} columns")
            else:
                print(f"  No parquet files found")

        print("\n" + "=" * 80)
        print(f"✓ Scan complete! Processed {len(datafeed_folders)} Datafeed folder(s)")
        print(f"✓ Extracted metadata from {len(all_results)} file(s)/sheet(s)")
        print("=" * 80)

        # Create DataFrame
        if all_results:
            df = pd.DataFrame(all_results)
            return df
        else:
            return pd.DataFrame()

    def export_to_csv(self, df, output_filename=None):
        """Export DataFrame to CSV.

        Args:
            df: DataFrame to export
            output_filename: Optional custom filename
        """
        if df.empty:
            print("\nNo data to export.")
            return

        # Generate filename with timestamp if not provided
        if not output_filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_filename = f"datafeed_report_{timestamp}.csv"

        try:
            df.to_csv(output_filename, index=False)
            print(f"\n✓ Report exported to: {output_filename}")
            print(f"  Total rows: {len(df)}")
            print(f"  Columns: {', '.join(df.columns.tolist())}")
        except Exception as e:
            print(f"\n✗ Error exporting CSV: {e}")


def main():
    """Main function."""
    print("=" * 80)
    print("Datafeed Scanner - Azure Blob Storage")
    print("Container: 30-projects")
    print("=" * 80)
    print()

    # Initialize scanner
    scanner = DatafeedScanner()

    # Generate report
    df = scanner.generate_report()

    # Display summary
    if not df.empty:
        print("\n" + "=" * 80)
        print("REPORT PREVIEW")
        print("=" * 80)
        print(df.to_string(index=False, max_rows=10))

        if len(df) > 10:
            print(f"\n... ({len(df) - 10} more rows)")

        print("\n" + "=" * 80)
        print("SUMMARY STATISTICS")
        print("=" * 80)
        print(f"Total entries: {len(df)}")
        print(f"Unique Datafeed paths: {df['Path'].nunique()}")
        print(f"Excel sheets: {len(df[df['Source_Type'] == 'Excel'])}")
        print(f"Parquet files: {len(df[df['Source_Type'] == 'Parquet'])}")
        print("=" * 80)

        # Export to CSV
        scanner.export_to_csv(df)
    else:
        print("\nNo data found to report.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user. Goodbye!")
        sys.exit(0)

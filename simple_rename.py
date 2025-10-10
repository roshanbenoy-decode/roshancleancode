"""
Simple Rename Tool - Search and rename files in Azure Blob Storage
Uses search_blob.py for searching, displays results as DataFrames
"""
from search_blob import search_blobs
from azure_file_manager import AzureFileManager
import pandas as pd
import time


def search_and_show_results(manager, search_term):
    """Search for files and return results"""
    print(f"\nSearching for files containing: '{search_term}'")
    print("=" * 80)

    # Use search_blobs from search_blob.py - searches ALL blobs, files only
    container_client = manager.blob_service_client.get_container_client(manager.CONTAINER_NAME)
    blobs = container_client.list_blobs()

    results = []
    for blob in blobs:
        filename = blob.name.split('/')[-1]
        is_folder = blob.size == 0 and blob.name.endswith('/')

        # Search for term in filename (contains mode)
        if search_term.lower() in filename.lower() and not is_folder:
            results.append({
                'name': filename,
                'path': blob.name,
                'size_mb': blob.size / (1024 * 1024)
            })

    return results


def show_found_files_df(results):
    """Convert results to DataFrame and display with file type analysis"""
    if not results:
        print("No files found!")
        return None

    # Create DataFrame
    df = pd.DataFrame(results)

    # Add File Type column
    df['File Type'] = df['name'].apply(lambda x: x.split('.')[-1] if '.' in x else 'no extension')

    # Reorder columns for better display
    df = df[['name', 'File Type', 'path', 'size_mb']]
    df.columns = ['Filename', 'File Type', 'Full Path', 'Size (MB)']

    # Round size
    df['Size (MB)'] = df['Size (MB)'].round(2)

    print(f"\nFound {len(df)} file(s):")
    print("=" * 80)
    print(df.to_string(index=False))
    print("=" * 80)

    # File type summary
    type_counts = df['File Type'].value_counts()
    if len(type_counts) == 1:
        print(f"\nFile Type: All files are .{type_counts.index[0]} ({type_counts.values[0]} files)")
    else:
        print("\nFile Types (Mixed):")
        for file_type, count in type_counts.items():
            print(f"  - .{file_type}: {count} file(s)")
    print()

    return df


def rename_files(manager, results, search_term, replace_term):
    """Rename files by replacing search_term with replace_term"""
    print("\n" + "=" * 80)
    print("Starting rename process...")
    print("=" * 80)

    rename_results = []
    container_client = manager.blob_service_client.get_container_client(manager.CONTAINER_NAME)

    for idx, file in enumerate(results, 1):
        old_path = file['path']
        old_name = file['name']

        # Create new name by replacing search term
        new_name = old_name.replace(search_term, replace_term)
        new_path = old_path.replace(search_term, replace_term)

        print(f"\n[{idx}/{len(results)}] Renaming...")
        print(f"  From: {old_name}")
        print(f"  To:   {new_name}")

        try:
            # Copy to new name
            source_blob = container_client.get_blob_client(old_path)
            dest_blob = container_client.get_blob_client(new_path)
            dest_blob.start_copy_from_url(source_blob.url)

            # Wait for copy
            time.sleep(2)

            # Delete old blob
            source_blob.delete_blob()

            rename_results.append({
                'Old Name': old_name,
                'New Name': new_name,
                'Old Path': old_path,
                'New Path': new_path,
                'Status': 'Success'
            })
            print("  ✓ Success")

        except Exception as e:
            rename_results.append({
                'Old Name': old_name,
                'New Name': new_name,
                'Old Path': old_path,
                'New Path': new_path,
                'Status': f'Failed: {str(e)[:50]}'
            })
            print(f"  ✗ Failed: {e}")

    return rename_results


def show_rename_results_df(rename_results):
    """Display before/after DataFrame and save to CSV"""
    df = pd.DataFrame(rename_results)

    print("\n" + "=" * 80)
    print("RENAME RESULTS")
    print("=" * 80)
    print(df.to_string(index=False))
    print("=" * 80)

    # Save to CSV
    csv_file = "rename_results.csv"
    df.to_csv(csv_file, index=False)
    print(f"\nResults saved to: {csv_file}")

    # Summary
    success_count = sum(1 for r in rename_results if r['Status'] == 'Success')
    failed_count = len(rename_results) - success_count

    print(f"\nSummary:")
    print(f"  Total processed: {len(rename_results)}")
    print(f"  Successful: {success_count}")
    print(f"  Failed: {failed_count}")


def main():
    print("=" * 80)
    print("SIMPLE RENAME TOOL")
    print("=" * 80)

    # Step 1: Get search term
    search_term = input("\nEnter search term to find files: ").strip()
    if not search_term:
        print("Search term cannot be empty!")
        return

    # Step 2: Initialize Azure and search
    print("\nInitializing Azure connection...")
    manager = AzureFileManager()

    results = search_and_show_results(manager, search_term)

    # Step 3: Show DataFrame
    df = show_found_files_df(results)
    if df is None:
        return

    # Step 4: User confirmation
    proceed = input("\nProceed with renaming these files? (yes/no): ").strip().lower()
    if proceed != 'yes':
        print("Rename cancelled.")
        return

    # Step 5: Get replacement term
    replace_term = input(f"\nReplace '{search_term}' with: ").strip()
    if not replace_term:
        print("Replacement term cannot be empty!")
        return

    # Step 6: Rename files
    rename_results = rename_files(manager, results, search_term, replace_term)

    # Step 7: Show results DataFrame
    show_rename_results_df(rename_results)

    print("\n" + "=" * 80)
    print("Done!")
    print("=" * 80)


if __name__ == "__main__":
    main()

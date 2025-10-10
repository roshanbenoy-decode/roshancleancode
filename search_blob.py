"""
Azure Blob Search - Find files/folders by name prefix in 30-projects container
"""
from azure_file_manager import AzureFileManager
import sys


def get_search_mode():
    """Prompt user to choose search mode"""
    print("\nSearch Mode:")
    print("1. Starts with (filename begins with search term)")
    print("2. Contains (filename contains search term anywhere)")

    while True:
        choice = input("\nSelect search mode (1 or 2): ").strip()
        if choice == '1':
            return 'starts_with'
        elif choice == '2':
            return 'contains'
        else:
            print("Invalid choice. Please enter 1 or 2.")


def get_type_filter():
    """Prompt user to choose type filter"""
    print("\nSearch Type:")
    print("1. All (files and folders)")
    print("2. Files only")
    print("3. Folders only")

    while True:
        choice = input("\nSelect type (1, 2, or 3): ").strip()
        if choice == '1':
            return 'all'
        elif choice == '2':
            return 'files'
        elif choice == '3':
            return 'folders'
        else:
            print("Invalid choice. Please enter 1, 2, or 3.")


def search_blobs(manager, search_term, search_mode='starts_with', type_filter='all'):
    """Search for blobs based on search mode and type filter"""
    # Display search criteria
    mode_text = "starts with" if search_mode == 'starts_with' else "contains"
    filter_text = {'all': 'files and folders', 'files': 'files only', 'folders': 'folders only'}[type_filter]

    print(f"\nSearching for items that {mode_text}: '{search_term}' ({filter_text})")
    print("-" * 80)

    results = []
    container_client = manager.blob_service_client.get_container_client(manager.CONTAINER_NAME)
    blobs = container_client.list_blobs()

    for blob in blobs:
        filename = blob.name.split('/')[-1]
        is_folder = blob.size == 0 and blob.name.endswith('/')

        # Apply search mode
        if search_mode == 'starts_with':
            matches = filename.lower().startswith(search_term.lower())
        else:  # contains
            matches = search_term.lower() in filename.lower()

        if not matches:
            continue

        # Apply type filter
        if type_filter == 'files' and is_folder:
            continue
        elif type_filter == 'folders' and not is_folder:
            continue

        results.append({
            'name': filename,
            'path': blob.name,
            'size_mb': blob.size / (1024 * 1024),
            'is_folder': is_folder
        })

    if not results:
        print(f"No items found matching your search criteria")
        return

    print(f"Found {len(results)} result(s):\n")

    for idx, item in enumerate(results, 1):
        item_type = "FOLDER" if item['is_folder'] else "FILE"
        size_info = "" if item['is_folder'] else f" ({item['size_mb']:.2f} MB)"
        print(f"{idx}. [{item_type}] {item['name']}{size_info}")
        print(f"   Path: {item['path']}")
        print()


def main():
    # Get search term
    if len(sys.argv) > 1:
        search_term = ' '.join(sys.argv[1:])
    else:
        search_term = input("\nEnter search term: ").strip()

    if not search_term:
        print("Search term cannot be empty")
        return

    # Get search mode
    search_mode = get_search_mode()

    # Get type filter
    type_filter = get_type_filter()

    print("\nInitializing Azure connection...")
    manager = AzureFileManager()
    search_blobs(manager, search_term, search_mode, type_filter)


if __name__ == "__main__":
    main()

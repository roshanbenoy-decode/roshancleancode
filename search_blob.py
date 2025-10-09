"""
Azure Blob Search - Find files/folders by name prefix in 30-projects container
"""
from azure_file_manager import AzureFileManager
import sys


def search_blobs(manager, search_term):
    """Search for blobs starting with the given term"""
    print(f"\nSearching for items starting with: '{search_term}'")
    print("-" * 80)

    results = []
    container_client = manager.blob_service_client.get_container_client(manager.CONTAINER_NAME)
    blobs = container_client.list_blobs()

    for blob in blobs:
        filename = blob.name.split('/')[-1]

        if filename.lower().startswith(search_term.lower()):
            results.append({
                'name': filename,
                'path': blob.name,
                'size_mb': blob.size / (1024 * 1024),
                'is_folder': blob.size == 0 and blob.name.endswith('/')
            })

    if not results:
        print(f"No files or folders found starting with '{search_term}'")
        return

    print(f"Found {len(results)} result(s):\n")

    for idx, item in enumerate(results, 1):
        item_type = "FOLDER" if item['is_folder'] else "FILE"
        size_info = "" if item['is_folder'] else f" ({item['size_mb']:.2f} MB)"
        print(f"{idx}. [{item_type}] {item['name']}{size_info}")
        print(f"   Path: {item['path']}")
        print()


def main():
    if len(sys.argv) > 1:
        search_term = ' '.join(sys.argv[1:])
    else:
        search_term = input("Enter search term (file/folder name prefix): ").strip()

    if not search_term:
        print("Search term cannot be empty")
        return

    print("Initializing Azure connection...")
    manager = AzureFileManager()
    search_blobs(manager, search_term)


if __name__ == "__main__":
    main()

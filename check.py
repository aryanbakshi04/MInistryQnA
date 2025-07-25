# Add this to a test script
from src.azure_vector_store import AzureVectorStore

vector_store = AzureVectorStore()
indexed_ministries = list(vector_store.indexed_ministries)

print("Currently indexed ministries:")
for i, ministry in enumerate(sorted(indexed_ministries), 1):
    count = vector_store.get_ministry_document_count(ministry)
    print(f"{i:2d}. {ministry} ({count} documents)")
import sys
import logging
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent.parent))

from src.config import Config
from src.azure_vector_store import AzureVectorStore
from src.azure_storage import AzureBlobStorage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_database_connection():
    """Test PostgreSQL database connection"""
    try:
        logger.info("Testing PostgreSQL connection...")
        vector_store = AzureVectorStore()
        
        # Test basic operations
        ministries = list(vector_store.indexed_ministries)
        logger.info(f"Database connection successful!")
        logger.info(f"Found {len(ministries)} indexed ministries: {ministries}")
        
        return True
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return False

def test_blob_storage_connection():
    """Test Azure Blob Storage connection"""
    try:
        logger.info("Testing Azure Blob Storage connection...")
        storage = AzureBlobStorage()
        
        # Test list operation
        pdfs = storage.list_pdfs()
        logger.info(f"✅ Blob storage connection successful!")
        logger.info(f"Found {len(pdfs)} PDF files in storage")
        
        return True
    except Exception as e:
        logger.error(f"Blob storage connection failed: {e}")
        return False

def main():
    """Run all connection tests"""
    logger.info("Starting connection tests...")
    
    # Validate environment first
    if not Config.validate_environment():
        logger.error("Environment validation failed")
        return False
    
    db_success = test_database_connection()
    storage_success = test_blob_storage_connection()
    
    if db_success and storage_success:
        logger.info("All connection tests passed!")
        return True
    else:
        logger.error("Some connection tests failed")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

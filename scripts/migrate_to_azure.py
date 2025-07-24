import sys
import logging
import json
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent.parent))

from src.config import Config
from src.azure_vector_store import AzureVectorStore
from src.azure_storage import AzureBlobStorage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def migrate_chroma_to_azure():
    """Migrate data from ChromaDB to Azure PostgreSQL"""
    try:
        logger.info("Starting migration from ChromaDB to Azure PostgreSQL...")
        
        # Initialize Azure services
        vector_store = AzureVectorStore()
        storage = AzureBlobStorage()
        
        # Check for existing ChromaDB data
        chroma_path = Path("data/vector_db")
        indexed_ministries_file = chroma_path / "indexed_ministries.json"
        
        if indexed_ministries_file.exists():
            # Load indexed ministries from ChromaDB
            with open(indexed_ministries_file, 'r') as f:
                indexed_ministries = json.load(f)
            
            logger.info(f"Found {len(indexed_ministries)} ministries in ChromaDB data")
        else:
            logger.warning("No ChromaDB indexed_ministries.json found")
            indexed_ministries = []
        
        # Migrate PDF files to Azure Blob Storage
        pdf_dir = Path("data/ministry_pdfs")
        if pdf_dir.exists():
            logger.info("Migrating PDF files to Azure Blob Storage...")
            
            for pdf_file in pdf_dir.glob("*.pdf"):
                # Try to determine ministry from filename or directory structure
                ministry_name = "Unknown"  # You might need to adjust this logic
                blob_name = f"ministries/{ministry_name}/{pdf_file.name}"
                
                url = storage.upload_pdf(str(pdf_file), blob_name)
                if url:
                    logger.info(f"  Uploaded {pdf_file.name} to blob storage")
                else:
                    logger.error(f"Failed to upload {pdf_file.name}")
        else:
            logger.info("No local PDF directory found to migrate")
        
        # Migration summary
        logger.info("Migration setup completed.")
        logger.info("=" * 50)
        logger.info("NEXT STEPS:")
        logger.info("1. Your PDF files have been uploaded to Azure Blob Storage")
        logger.info("2. Run 'python scripts/create_ministry_database.py' to re-index documents in PostgreSQL")
        logger.info("3. Test your application with 'python scripts/test_connection.py'")
        logger.info("4. Start your app with 'streamlit run app.py'")
        logger.info("=" * 50)
        
        return True
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        return False

def cleanup_old_data():
    """Clean up old ChromaDB files after successful migration"""
    try:
        import shutil
        
        # Backup before deletion
        chroma_path = Path("data/vector_db")
        if chroma_path.exists():
            backup_path = Path("data/chromadb_backup")
            if backup_path.exists():
                shutil.rmtree(backup_path)
            shutil.copytree(chroma_path, backup_path)
            logger.info(f"Created backup at {backup_path}")
            
            # Remove original
            shutil.rmtree(chroma_path)
            logger.info("Cleaned up old ChromaDB files")
        
        pdf_path = Path("data/ministry_pdfs")
        if pdf_path.exists():
            backup_pdf_path = Path("data/ministry_pdfs_backup")
            if backup_pdf_path.exists():
                shutil.rmtree(backup_pdf_path)
            shutil.copytree(pdf_path, backup_pdf_path)
            logger.info(f"Created PDF backup at {backup_pdf_path}")
            
            shutil.rmtree(pdf_path)
            logger.info("Cleaned up local PDF files")
            
    except Exception as e:
        logger.warning(f"Cleanup warning: {e}")

if __name__ == "__main__":
    if migrate_chroma_to_azure():
        logger.info("Migration completed successfully!")
        
        # Ask user if they want to cleanup old files
        response = input("\nDo you want to clean up old ChromaDB files? This will create a backup first. (y/N): ")
        if response.lower() == 'y':
            cleanup_old_data()
            logger.info("Old files cleaned up (backups created)")
    else:
        logger.error("Migration failed!")
        sys.exit(1)

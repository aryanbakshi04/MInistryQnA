import sys
import asyncio
import logging
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent.parent))

from src.config import Config
from src.azure_vector_store import AzureVectorStore
from src.azure_storage import AzureBlobStorage
from src.document_processor import DocumentProcessor
from src.sansad_client import SansadClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MinistryDatabaseCreator:
    def __init__(self):
        self.vector_store = AzureVectorStore()
        self.storage = AzureBlobStorage()
        self.document_processor = DocumentProcessor()
        self.sansad_client = SansadClient()
        
    async def create_ministry_database(self, ministries: list):
        """Create database for specified ministries"""
        logger.info(f"Starting database creation for {len(ministries)} ministries...")
        
        for ministry in ministries:
            try:
                logger.info(f"Processing {ministry}...")
                
                # Check if ministry is already indexed
                if self.vector_store.is_ministry_indexed(ministry):
                    logger.info(f"{ministry} is already indexed. Skipping...")
                    continue
                
                # Get PDF files for this ministry from blob storage
                pdf_blobs = self.storage.list_pdfs(f"ministries/{ministry}/")
                
                if not pdf_blobs:
                    logger.warning(f"No PDFs found for {ministry} in blob storage")
                    
                    # Try to fetch PDFs from Sansad API
                    logger.info(f"Attempting to fetch PDFs for {ministry} from Sansad API...")
                    await self._fetch_and_process_ministry_pdfs(ministry)
                    continue
                
                # Process each PDF blob
                all_documents = []
                for blob_name in pdf_blobs:
                    logger.info(f"Processing PDF: {blob_name}")
                    
                    documents = self.document_processor.process_pdf_from_blob(blob_name, ministry)
                    if documents:
                        all_documents.extend(documents)
                        logger.info(f"Extracted {len(documents)} chunks from {blob_name}")
                    else:
                        logger.warning(f"No content extracted from {blob_name}")
                
                # Add documents to vector store
                if all_documents:
                    logger.info(f"Adding {len(all_documents)} documents to vector store for {ministry}")
                    self.vector_store.add_documents(all_documents, ministry)
                    logger.info(f"✅ Successfully indexed {ministry} with {len(all_documents)} documents")
                else:
                    logger.warning(f"No documents to index for {ministry}")
                    
            except Exception as e:
                logger.error(f"Error processing {ministry}: {e}")
                continue
        
        logger.info("Database creation completed!")
        
    async def _fetch_and_process_ministry_pdfs(self, ministry: str):
        """Fetch PDFs from Sansad API and process them"""
        try:
            # Get PDF URLs from Sansad client
            pdf_urls = await self.sansad_client.get_ministry_pdf_urls(ministry)
            
            if not pdf_urls:
                logger.warning(f"No PDF URLs found for {ministry}")
                return
            
            logger.info(f"Found {len(pdf_urls)} PDFs for {ministry}")
            
            # Download and process each PDF
            all_documents = []
            for i, pdf_url in enumerate(pdf_urls[:5]):  # Limit to first 5 for testing
                try:
                    logger.info(f"Processing PDF {i+1}/{len(pdf_urls)}: {pdf_url}")
                    
                    # Download PDF to local cache
                    local_path = await self.sansad_client.download_pdf(pdf_url)
                    if not local_path:
                        continue
                        
                    # Process PDF locally
                    documents = self.document_processor.process_local_pdf(local_path, ministry)
                    if documents:
                        all_documents.extend(documents)
                    
                    # Upload to blob storage for future use
                    blob_name = f"ministries/{ministry}/{Path(local_path).name}"
                    self.storage.upload_pdf(local_path, blob_name)
                    
                except Exception as e:
                    logger.error(f"Error processing PDF {pdf_url}: {e}")
                    continue
            
            # Add all documents to vector store
            if all_documents:
                self.vector_store.add_documents(all_documents, ministry)
                logger.info(f"✅ Successfully indexed {ministry} with {len(all_documents)} documents")
                
        except Exception as e:
            logger.error(f"Error fetching PDFs for {ministry}: {e}")

async def main():
    """Main function to create ministry database"""
    
    # List of ministries to process (you can modify this list)
    
    
    creator = MinistryDatabaseCreator()
    await creator.create_ministry_database(Config.MINISTRIES)
    
    logger.info("🎉 Ministry database creation completed!")

if __name__ == "__main__":
    asyncio.run(main())

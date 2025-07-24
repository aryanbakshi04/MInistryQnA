import sys
import logging
import asyncio
import aiohttp
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent.parent))

from src.config import Config
from src.azure_storage import AzureBlobStorage
from src.sansad_client import SansadClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MinistryPDFFetcher:
    def __init__(self):
        self.storage = AzureBlobStorage()
        self.sansad_client = SansadClient()
        
    async def fetch_and_upload_pdfs(self, ministry_list: list):
        """Fetch PDFs and upload directly to Azure Blob Storage"""
        try:
            for ministry in ministry_list:
                logger.info(f"Processing ministry: {ministry}")
                
                # Get PDF URLs from Sansad client
                pdf_urls = await self.sansad_client.get_ministry_pdf_urls(ministry)
                
                if not pdf_urls:
                    logger.warning(f"No PDFs found for {ministry}")
                    continue
                
                for pdf_url in pdf_urls:
                    await self._download_and_upload_pdf(ministry, pdf_url)
                    
        except Exception as e:
            logger.error(f"Error in PDF fetching process: {e}")

    async def _download_and_upload_pdf(self, ministry: str, pdf_url: str):
        """Download PDF and upload to Azure Blob Storage"""
        try:
            # Generate blob name
            filename = pdf_url.split('/')[-1]
            if not filename.endswith('.pdf'):
                filename += '.pdf'
            
            blob_name = f"ministries/{ministry}/{filename}"
            
            # Check if already exists
            if self.storage.pdf_exists(blob_name):
                logger.info(f"PDF already exists: {blob_name}")
                return
            
            # Download PDF
            async with aiohttp.ClientSession() as session:
                async with session.get(pdf_url) as response:
                    if response.status == 200:
                        # Save to temporary location
                        temp_path = f"data/pdf_cache/{filename}"
                        Path(temp_path).parent.mkdir(parents=True, exist_ok=True)
                        
                        with open(temp_path, 'wb') as f:
                            f.write(await response.read())
                        
                        # Upload to Azure Blob Storage
                        url = self.storage.upload_pdf(temp_path, blob_name)
                        if url:
                            logger.info(f"Successfully uploaded {blob_name}")
                            
                            # Clean up temp file
                            Path(temp_path).unlink()
                        else:
                            logger.error(f"Failed to upload {blob_name}")
                    else:
                        logger.error(f"Failed to download PDF: {pdf_url} (Status: {response.status})")
                        
        except Exception as e:
            logger.error(f"Error processing PDF {pdf_url}: {e}")

async def main():
    """Main function to fetch ministry PDFs"""
    # List of ministries to process
    ministries = [
        "Ministry of Education",
        "Ministry of Health and Family Welfare",
        "Ministry of Finance",
        "Ministry of Defence",
        "Ministry of Home Affairs",
        # Add more ministries as needed
    ]
    
    fetcher = MinistryPDFFetcher()
    await fetcher.fetch_and_upload_pdfs(ministries)
    logger.info("PDF fetching process completed!")

if __name__ == "__main__":
    asyncio.run(main())

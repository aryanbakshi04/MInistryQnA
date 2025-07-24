import logging
import os
from pathlib import Path
from typing import Optional, List
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.core.exceptions import ResourceNotFoundError, AzureError
from .config import Config

logger = logging.getLogger(__name__)

class AzureBlobStorage:
    def __init__(self):
        """Initialize Azure Blob Storage client"""
        if not Config.AZURE_STORAGE_CONNECTION_STRING:
            raise ValueError("AZURE_STORAGE_CONNECTION_STRING environment variable is required")
        
        self.blob_service_client = BlobServiceClient.from_connection_string(
            Config.AZURE_STORAGE_CONNECTION_STRING
        )
        self.container_name = Config.AZURE_STORAGE_CONTAINER
        self._ensure_container_exists()

    def _ensure_container_exists(self):
        """Create container if it doesn't exist"""
        try:
            self.blob_service_client.create_container(self.container_name)
            logger.info(f"Created container: {self.container_name}")
        except Exception as e:
            # Container likely already exists
            logger.debug(f"Container creation result: {e}")

    def upload_pdf(self, file_path: str, blob_name: str) -> Optional[str]:
        """Upload PDF file to blob storage"""
        try:
            if not os.path.exists(file_path):
                logger.error(f"File not found: {file_path}")
                return None
            
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name, 
                blob=blob_name
            )
            
            with open(file_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
            
            logger.info(f"Successfully uploaded {file_path} as {blob_name}")
            return blob_client.url
            
        except AzureError as e:
            logger.error(f"Azure error uploading {file_path}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error uploading {file_path}: {e}")
            return None

    def download_pdf(self, blob_name: str, download_path: str) -> bool:
        """Download PDF from blob storage"""
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name, 
                blob=blob_name
            )
            
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(download_path), exist_ok=True)
            
            with open(download_path, "wb") as download_file:
                download_data = blob_client.download_blob()
                download_file.write(download_data.readall())
            
            logger.info(f"Successfully downloaded {blob_name} to {download_path}")
            return True
            
        except ResourceNotFoundError:
            logger.error(f"Blob not found: {blob_name}")
            return False
        except AzureError as e:
            logger.error(f"Azure error downloading {blob_name}: {e}")
            return False
        except Exception as e:
            logger.error(f"Error downloading {blob_name}: {e}")
            return False

    def list_pdfs(self, prefix: str = "") -> List[str]:
        """List all PDF files in the container"""
        try:
            container_client = self.blob_service_client.get_container_client(
                self.container_name
            )
            
            blob_names = []
            for blob in container_client.list_blobs(name_starts_with=prefix):
                if blob.name.lower().endswith('.pdf'):
                    blob_names.append(blob.name)
            
            logger.info(f"Found {len(blob_names)} PDF files with prefix '{prefix}'")
            return blob_names
            
        except AzureError as e:
            logger.error(f"Azure error listing blobs: {e}")
            return []
        except Exception as e:
            logger.error(f"Error listing blobs: {e}")
            return []

    def delete_pdf(self, blob_name: str) -> bool:
        """Delete PDF from blob storage"""
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name, 
                blob=blob_name
            )
            
            blob_client.delete_blob()
            logger.info(f"Successfully deleted {blob_name}")
            return True
            
        except ResourceNotFoundError:
            logger.warning(f"Blob not found for deletion: {blob_name}")
            return True  # Already deleted
        except AzureError as e:
            logger.error(f"Azure error deleting {blob_name}: {e}")
            return False
        except Exception as e:
            logger.error(f"Error deleting {blob_name}: {e}")
            return False

    def pdf_exists(self, blob_name: str) -> bool:
        """Check if PDF exists in blob storage"""
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name, 
                blob=blob_name
            )
            return blob_client.exists()
        except Exception as e:
            logger.error(f"Error checking existence of {blob_name}: {e}")
            return False

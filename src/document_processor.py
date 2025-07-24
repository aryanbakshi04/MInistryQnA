import logging
import re
from typing import List, Dict, Any
from pathlib import Path
from pypdf import PdfReader
from .azure_storage import AzureBlobStorage

logger = logging.getLogger(__name__)

class DocumentProcessor:
    def __init__(self):
        """Initialize Document Processor with Azure Storage"""
        self.storage = AzureBlobStorage()
        
    def extract_text_from_pdf(self, pdf_path: str) -> str:
        """Extract text from PDF file"""
        try:
            with open(pdf_path, 'rb') as file:
                pdf_reader = PdfReader(file)
                text = ""
                
                for page_num, page in enumerate(pdf_reader.pages, 1):
                    try:
                        page_text = page.extract_text()
                        if page_text:
                            text += f"\n--- Page {page_num} ---\n{page_text}"
                    except Exception as e:
                        logger.warning(f"Error extracting text from page {page_num}: {e}")
                        continue
                
                return text.strip()
                
        except Exception as e:
            logger.error(f"Error extracting text from PDF {pdf_path}: {e}")
            return ""

    def clean_text(self, text: str) -> str:
        """Clean and normalize extracted text"""
        if not text:
            return ""
        
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove page headers/footers patterns
        text = re.sub(r'--- Page \d+ ---', '', text)
        
        # Remove special characters but keep basic punctuation
        text = re.sub(r'[^\w\s.,!?()-]', ' ', text)
        
        # Remove excessive spaces again
        text = re.sub(r'\s+', ' ', text)
        
        return text.strip()

    def chunk_text(self, text: str, chunk_size: int = 1000, overlap: int = 200) -> List[str]:
        """Split text into overlapping chunks"""
        if not text:
            return []
        
        # Split by sentences first
        sentences = re.split(r'[.!?]+', text)
        
        chunks = []
        current_chunk = ""
        
        for sentence in sentences:
            sentence = sentence.strip()
            if not sentence:
                continue
                
            # If adding this sentence would exceed chunk size
            if len(current_chunk) + len(sentence) > chunk_size:
                if current_chunk:
                    chunks.append(current_chunk.strip())
                    
                    # Create overlap with previous chunk
                    if overlap > 0 and len(current_chunk) > overlap:
                        current_chunk = current_chunk[-overlap:] + " " + sentence
                    else:
                        current_chunk = sentence
                else:
                    current_chunk = sentence
            else:
                current_chunk += " " + sentence if current_chunk else sentence
        
        # Add the last chunk
        if current_chunk.strip():
            chunks.append(current_chunk.strip())
        
        return chunks

    def process_pdf_from_blob(self, blob_name: str, ministry: str) -> List[Dict[str, Any]]:
        """Process PDF from Azure Blob Storage"""
        try:
            # Download PDF to temporary location
            temp_path = f"data/pdf_cache/{blob_name.split('/')[-1]}"
            
            if not self.storage.download_pdf(blob_name, temp_path):
                logger.error(f"Failed to download PDF: {blob_name}")
                return []
            
            # Extract text
            raw_text = self.extract_text_from_pdf(temp_path)
            if not raw_text:
                logger.warning(f"No text extracted from PDF: {blob_name}")
                return []
            
            # Clean text
            cleaned_text = self.clean_text(raw_text)
            
            # Split into chunks
            chunks = self.chunk_text(cleaned_text)
            
            # Create documents
            documents = []
            for i, chunk in enumerate(chunks):
                doc = {
                    "id": f"{blob_name.replace('/', '_')}_chunk_{i}",
                    "text": chunk,
                    "metadata": {
                        "source": blob_name,
                        "ministry": ministry,
                        "chunk_index": i,
                        "total_chunks": len(chunks)
                    }
                }
                documents.append(doc)
            
            logger.info(f"Processed {blob_name}: {len(documents)} chunks created")
            
            # Clean up temporary file
            try:
                Path(temp_path).unlink()
            except Exception as e:
                logger.warning(f"Failed to cleanup temp file {temp_path}: {e}")
            
            return documents
            
        except Exception as e:
            logger.error(f"Error processing PDF {blob_name}: {e}")
            return []

    def process_local_pdf(self, pdf_path: str, ministry: str) -> List[Dict[str, Any]]:
        """Process PDF from local filesystem (for migration purposes)"""
        try:
            if not Path(pdf_path).exists():
                logger.error(f"PDF file not found: {pdf_path}")
                return []
            
            # Extract and process text
            raw_text = self.extract_text_from_pdf(pdf_path)
            if not raw_text:
                return []
            
            cleaned_text = self.clean_text(raw_text)
            chunks = self.chunk_text(cleaned_text)
            
            # Create documents
            documents = []
            filename = Path(pdf_path).name
            
            for i, chunk in enumerate(chunks):
                doc = {
                    "id": f"{filename}_chunk_{i}",
                    "text": chunk,
                    "metadata": {
                        "source": filename,
                        "ministry": ministry,
                        "chunk_index": i,
                        "total_chunks": len(chunks)
                    }
                }
                documents.append(doc)
            
            return documents
            
        except Exception as e:
            logger.error(f"Error processing local PDF {pdf_path}: {e}")
            return []

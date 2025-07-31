import sys
import requests
import json
import logging
import time
from io import BytesIO
from pathlib import Path
import sqlalchemy as sa

sys.path.append(str(Path(__file__).parent.parent))

from src.config import Config
from src.azure_storage import AzureBlobStorage
from src.azure_vector_store import AzureVectorStore
from src.document_processor import DocumentProcessor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MINISTRY_CODE_TO_NAME = {
    59: "Ministry of Electronics and Information Technology",
    12: "Ministry of Education",
    39: "Ministry of Finance",
}

API_URL = "https://sansad.in/api_ls/question/qetFilteredQuestionsAns"


class SansadClient:
    def __init__(self, ministry_code, session_number=5, loksabha_no=18, page_size=20):
        self.ministry_code = ministry_code
        self.ministry = MINISTRY_CODE_TO_NAME[ministry_code]
        self.session_number = session_number
        self.loksabha_no = loksabha_no
        self.page_size = page_size
        self.vector_store = AzureVectorStore()
        self.document_processor = DocumentProcessor()
        self.storage = AzureBlobStorage()

    def fetch_all_questions(self):
        all_questions = []
        page_no = 1
        total = None
        while True:
            params = {
                "loksabhaNo": str(self.loksabha_no),
                "sessionNumber": str(self.session_number),
                "pageNo": str(page_no),
                "locale": "en",
                "pageSize": str(self.page_size),
                "ministryCode": str(self.ministry_code),
            }
            r = requests.get(API_URL, params=params, timeout=40)
            if r.status_code != 200:
                logger.error(f"API request failed: {r.status_code} {r.text}")
                break
            data = r.json()
            if not data or not data[0]["listOfQuestions"]:
                break
            questions = data[0]["listOfQuestions"]
            total = int(data[0]["totalRecordSize"])
            all_questions.extend(questions)
            logger.info(f"{self.ministry} page {page_no}: {len(questions)} questions")
            if len(all_questions) >= total:
                break
            page_no += 1
            time.sleep(0.25)
        logger.info(f"Fetched {len(all_questions)} total questions for {self.ministry}")
        return all_questions

    def is_pdf_in_db(self, filename):
        session = self.vector_store.Session()
        try:
            existing = session.execute(
                sa.text(
                    "SELECT 1 FROM documents WHERE doc_metadata->>'source' = :filename LIMIT 1"
                ),
                {"filename": filename},
            )
            return existing.first() is not None
        finally:
            session.close()

    def fetch_pdf_bytes(self, url):
        try:
            r = requests.get(url, stream=True, timeout=60)
            r.raise_for_status()
            return r.content
        except Exception as e:
            logger.error(f"Failed to fetch PDF bytes from {url}: {e}")
            return None

    def process_pdf_from_bytes(self, pdf_bytes, filename, pdf_url=None):
        try:
            reader = self.document_processor.get_pdf_reader_from_bytes(pdf_bytes)
            text = self.document_processor.extract_text_from_pdf_reader(reader)
            if not text:
                logger.warning(f"No text extracted from PDF {filename}")
                return []
            cleaned_text = self.document_processor.clean_text(text)
            chunks = self.document_processor.chunk_text(cleaned_text)
            documents = []
            for i, chunk in enumerate(chunks):
                doc = {
                    "id": f"{filename}_chunk_{i}",
                    "text": chunk,
                    "metadata": {
                        "source": filename,
                        "original_url": pdf_url,  # Store original URL
                        "ministry": self.ministry,
                        "chunk_index": i,
                        "total_chunks": len(chunks),
                    },
                }
                documents.append(doc)
            logger.info(f"Processed PDF {filename}, created {len(documents)} chunks")
            return documents
        except Exception as e:
            logger.error(f"Error processing PDF {filename} from bytes: {e}")
            return []


    def ingest(self):
        questions = self.fetch_all_questions()

        ingested_files = 0
        for q in questions:
            try:
                pdf_url = q.get("questionsFilePath")
                if not pdf_url:
                    continue
                filename = pdf_url.split("/")[-1].split("?")[0]
                if self.is_pdf_in_db(filename):
                    logger.info(f"{filename} already in database. Skipping.")
                    continue
                pdf_bytes = self.fetch_pdf_bytes(pdf_url)
                if pdf_bytes is None:
                    continue
                documents = self.process_pdf_from_bytes(pdf_bytes, filename,pdf_url)
                if not documents:
                    continue
                try:
                    self.storage.upload_bytes(pdf_bytes, f"{self.ministry}/{filename}")
                    logger.info(f"Uploaded {filename} to Azure Blob Storage")
                except Exception as e:
                    logger.error(f"Failed to upload {filename} to blob storage: {e}")
                self.vector_store.add_documents(documents, self.ministry)
                ingested_files += 1
                logger.info(f"Ingested {filename} with {len(documents)} chunks")
                time.sleep(0.3)
            except Exception as e:
                logger.error(f"Error processing question with PDF {pdf_url}: {e}")
                continue
        logger.info(f"Completed ingest for {self.ministry}: {ingested_files} new files")

    @staticmethod
    def fetch_selected_ministries():
        for code in MINISTRY_CODE_TO_NAME:
            ingestor = SansadClient(code)
            ingestor.ingest()    


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1].isdigit():
        code = int(sys.argv[1])
        SansadClient(code).ingest()
    else:
        SansadClient.fetch_selected_ministries()

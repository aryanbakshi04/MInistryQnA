import logging
import json
from typing import List, Dict, Any, Optional
from datetime import datetime
import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError, PendingRollbackError
from pgvector.sqlalchemy import Vector
from sentence_transformers import SentenceTransformer
from .config import Config
import time

logger = logging.getLogger(__name__)

Base = declarative_base()


class Document(Base):
    __tablename__ = "documents"

    id = sa.Column(sa.String, primary_key=True)
    text = sa.Column(sa.Text, nullable=False)
    embedding = sa.Column(Vector(384))
    doc_metadata = sa.Column(sa.JSON)
    ministry = sa.Column(sa.String, index=True)
    created_at = sa.Column(sa.DateTime, default=datetime.utcnow)
    updated_at = sa.Column(
        sa.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )


class AzureVectorStore:
    def __init__(self):

        if not Config.POSTGRESQL_URL:
            raise ValueError("POSTGRESQL_URL environment variable is required")

        self.engine = sa.create_engine(
            Config.POSTGRESQL_URL,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=1800,
            pool_pre_ping=True,
            connect_args={"application_name": "ministryDB_connection"},
        )

        Base.metadata.create_all(self.engine)

        self.Session = sessionmaker(bind=self.engine, expire_on_commit=False)
        self.model = SentenceTransformer(Config.EMBEDDING_MODEL)
        self.indexed_ministries = set()
        self._load_indexed_ministries()

    def _load_indexed_ministries(self):

        max_retries = 3
        for attempt in range(max_retries):
            try:
                with self.Session() as session:
                    result = session.execute(
                        sa.text(
                            "SELECT DISTINCT ministry FROM documents WHERE ministry IS NOT NULL"
                        )
                    )
                    self.indexed_ministries = {row[0] for row in result}
                    logger.info(
                        f"Loaded {len(self.indexed_ministries)} indexed ministries"
                    )
                    return
            except Exception as e:
                logger.warning(
                    f"Attempt {attempt + 1} failed to load indexed ministries: {e}"
                )
                if attempt < max_retries - 1:
                    time.sleep(2**attempt)  # Exponential backoff
                else:
                    logger.error("Failed to load indexed ministries after all retries")
                    self.indexed_ministries = set()

    def create_embedding(self, text: str) -> List[float]:

        try:
            if not text or not text.strip():
                raise ValueError("Text cannot be empty")
            return self.model.encode(text.strip()).tolist()
        except Exception as e:
            logger.error(f"Error creating embedding: {e}")
            raise

    def _safe_session_operation(self, operation_func, max_retries=3):

        for attempt in range(max_retries):
            session = None
            try:
                session = self.Session()
                result = operation_func(session)
                session.commit()
                return result

            except PendingRollbackError as e:
                logger.warning(
                    f"Transaction rollback required (attempt {attempt + 1}): {e}"
                )
                if session:
                    try:
                        session.rollback()
                        session.close()
                    except Exception as rollback_error:
                        logger.error(f"Error during rollback: {rollback_error}")

                if attempt < max_retries - 1:
                    time.sleep(2**attempt)
                    continue
                else:
                    raise

            except SQLAlchemyError as e:
                logger.error(f"Database error (attempt {attempt + 1}): {e}")
                if session:
                    try:
                        session.rollback()
                        session.close()
                    except Exception as rollback_error:
                        logger.error(f"Error during rollback: {rollback_error}")

                if attempt < max_retries - 1:
                    time.sleep(2**attempt)
                    continue
                else:
                    raise

            except Exception as e:
                logger.error(f"Unexpected error (attempt {attempt + 1}): {e}")
                if session:
                    try:
                        session.rollback()
                        session.close()
                    except Exception as rollback_error:
                        logger.error(f"Error during rollback: {rollback_error}")

                if attempt < max_retries - 1:
                    time.sleep(2**attempt)
                    continue
                else:
                    raise

            finally:
                if session:
                    try:
                        session.close()
                    except Exception as close_error:
                        logger.error(f"Error closing session: {close_error}")

    def add_documents_batch(
        self,
        documents: List[Dict[str, Any]],
        ministry: str = None,
        batch_size: int = 10,
    ):

        if not documents:
            logger.warning("No documents provided to add")
            return 0

        total_added = 0

        # Process documents in batches
        for i in range(0, len(documents), batch_size):
            batch = documents[i : i + batch_size]
            logger.info(
                f"Processing batch {i//batch_size + 1}/{(len(documents) + batch_size - 1)//batch_size} ({len(batch)} documents)"
            )

            def batch_operation(session):
                added_count = 0
                for doc in batch:
                    text = doc.get("text", "").strip()
                    if not text:
                        logger.warning(
                            f"Skipping document with empty text: {doc.get('id', 'unknown')}"
                        )
                        continue

                    try:

                        embedding = self.create_embedding(text)

                        db_doc = Document(
                            id=doc.get(
                                "id", f"doc_{datetime.now().timestamp()}_{added_count}"
                            ),
                            text=text,
                            embedding=embedding,
                            doc_metadata=doc.get("metadata", {}),
                            ministry=ministry
                            or doc.get("metadata", {}).get("ministry"),
                        )

                        # Use merge to handle potential duplicates
                        session.merge(db_doc)
                        added_count += 1

                    except Exception as e:
                        logger.error(
                            f"Error processing document {doc.get('id', 'unknown')}: {e}"
                        )
                        continue

                return added_count

            try:
                batch_added = self._safe_session_operation(batch_operation)
                total_added += batch_added
                logger.info(f"Successfully added {batch_added} documents in this batch")

                time.sleep(0.1)

            except Exception as e:
                logger.error(f"Failed to process batch {i//batch_size + 1}: {e}")
                continue

        if ministry and total_added > 0:
            self.indexed_ministries.add(ministry)

        logger.info(
            f"Successfully added {total_added} total documents for ministry: {ministry}"
        )
        return total_added

    def add_documents(self, documents: List[Dict[str, Any]], ministry: str = None):

        return self.add_documents_batch(documents, ministry, batch_size=10)

    def search_by_text(
        self, query: str, ministry: str, n_results: int = 10
    ) -> List[Dict[str, Any]]:

        if not query or not query.strip():
            logger.warning("Empty query provided")
            return []

        def search_operation(session):
            query_embedding = self.create_embedding(query.strip())

            stmt = sa.text(
                """
                SELECT id, text, doc_metadata, ministry, 
                       embedding <-> :query_embedding as distance
                FROM documents 
                WHERE ministry = :ministry
                ORDER BY embedding <-> :query_embedding
                LIMIT :limit
            """
            )

            result = session.execute(
                stmt,
                {
                    "query_embedding": str(query_embedding),
                    "ministry": ministry,
                    "limit": n_results,
                },
            )

            documents = []
            for row in result:
                documents.append(
                    {
                        "id": row.id,
                        "text": row.text,
                        "metadata": row.doc_metadata or {},
                        "ministry": row.ministry,
                        "distance": float(row.distance),
                        "relevance_score": max(0, 1.0 - float(row.distance)),
                    }
                )

            return documents

        try:
            documents = self._safe_session_operation(search_operation)
            logger.info(
                f"Found {len(documents)} relevant documents for query in {ministry}"
            )
            return documents
        except Exception as e:
            logger.error(f"Error searching documents: {e}")
            return []

    def is_ministry_indexed(self, ministry: str) -> bool:

        return ministry in self.indexed_ministries

    def get_ministry_document_count(self, ministry: str) -> int:

        def count_operation(session):
            result = session.execute(
                sa.text("SELECT COUNT(*) FROM documents WHERE ministry = :ministry"),
                {"ministry": ministry},
            )
            return result.scalar()

        try:
            return self._safe_session_operation(count_operation)
        except Exception as e:
            logger.error(f"Error getting document count for {ministry}: {e}")
            return 0

    def clear_ministry_documents(self, ministry: str):

        def clear_operation(session):
            result = session.execute(
                sa.text("DELETE FROM documents WHERE ministry = :ministry"),
                {"ministry": ministry},
            )
            return result.rowcount

        try:
            deleted_count = self._safe_session_operation(clear_operation)
            self.indexed_ministries.discard(ministry)
            logger.info(f"Cleared {deleted_count} documents for ministry: {ministry}")
        except Exception as e:
            logger.error(f"Error clearing documents for {ministry}: {e}")
            raise

    def clear_all(self):

        def clear_all_operation(session):
            result = session.execute(sa.text("DELETE FROM documents"))
            return result.rowcount

        try:
            deleted_count = self._safe_session_operation(clear_all_operation)
            self.indexed_ministries.clear()
            logger.info(f"Cleared {deleted_count} total documents from vector store")
        except Exception as e:
            logger.error(f"Error clearing all documents: {e}")
            raise

    def get_database_health(self):

        def health_operation(session):

            result = session.execute(sa.text("SELECT 1"))
            connectivity = result.scalar() == 1

            result = session.execute(sa.text("SELECT COUNT(*) FROM documents"))
            doc_count = result.scalar()

            result = session.execute(
                sa.text(
                    "SELECT COUNT(DISTINCT ministry) FROM documents WHERE ministry IS NOT NULL"
                )
            )
            ministry_count = result.scalar()

            return {
                "connectivity": connectivity,
                "total_documents": doc_count,
                "ministries_count": ministry_count,
                "indexed_ministries": list(self.indexed_ministries),
            }

        try:
            return self._safe_session_operation(health_operation)
        except Exception as e:
            logger.error(f"Error getting database health: {e}")
            return {
                "connectivity": False,
                "total_documents": 0,
                "ministries_count": 0,
                "indexed_ministries": [],
            }

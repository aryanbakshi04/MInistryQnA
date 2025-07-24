import sys
import logging
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent.parent))

from src.config import Config
from src.azure_vector_store import AzureVectorStore, Base
import sqlalchemy as sa

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def setup_database():
    """Initialize Azure PostgreSQL database for Parliamentary Q&A"""
    try:
        logger.info("Starting database setup...")
        
        # Validate environment
        if not Config.validate_environment():
            logger.error("Environment validation failed. Please check your .env file.")
            return False
        
        # Create engine
        engine = sa.create_engine(Config.POSTGRESQL_URL)
        
        # Test connection
        with engine.connect() as conn:
            result = conn.execute(sa.text("SELECT version()"))
            version = result.fetchone()[0]
            logger.info(f"Connected to PostgreSQL: {version}")
        
        # Create all tables
        logger.info("Creating database tables...")
        Base.metadata.create_all(engine)
        
        # Create vector extension if not exists
        with engine.connect() as conn:
            try:
                conn.execute(sa.text("CREATE EXTENSION IF NOT EXISTS vector"))
                conn.commit()
                logger.info("Vector extension ensured")
            except Exception as e:
                logger.warning(f"Vector extension issue: {e}")
        
        # Create indexes for better performance
        logger.info("Creating indexes...")
        with engine.connect() as conn:
            try:
                # Index on ministry for faster filtering
                conn.execute(sa.text("""
                    CREATE INDEX IF NOT EXISTS idx_documents_ministry 
                    ON documents(ministry)
                """))
                
                # Index on created_at for time-based queries
                conn.execute(sa.text("""
                    CREATE INDEX IF NOT EXISTS idx_documents_created_at 
                    ON documents(created_at)
                """))
                
                conn.commit()
                logger.info("Indexes created successfully")
            except Exception as e:
                logger.warning(f"Index creation issue: {e}")
        
        # Initialize vector store to create initial setup
        vector_store = AzureVectorStore()
        logger.info(f"Vector store initialized with {len(vector_store.indexed_ministries)} ministries")
        
        logger.info("Database setup completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Database setup failed: {e}")
        return False

if __name__ == "__main__":
    success = setup_database()
    sys.exit(0 if success else 1)

import streamlit as st
import logging
from src.config import Config
from src.azure_vector_store import AzureVectorStore  # Changed import
from src.document_processor import DocumentProcessor
from src.llm_client import LLMClient  # Your existing LLM client
from src.sansad_client import SansadClient

# Configure logging
logging.basicConfig(level=Config.get_log_level())
logger = logging.getLogger(__name__)

def main():
    st.set_page_config(
        page_title="Parliamentary Q&A Assistant",
        page_icon="🏛️",
        layout="wide"
    )
    
    st.title("Parliamentary Q&A Assistant")
    st.markdown("Ask questions about Indian Parliamentary proceedings and get AI-powered answers.")
    
    # Initialize components
    if not Config.validate_environment():
        st.error("Missing required environment variables. Please check your configuration.")
        st.stop()
    
    try:
        # Initialize services
        vector_store = AzureVectorStore()  # Using PostgreSQL now
        llm_client = LLMClient()
        sansad_client = SansadClient()
        
        # Sidebar for ministry selection
        st.sidebar.header("Settings")
        
        # Get available ministries
        ministries = list(vector_store.indexed_ministries)
        if not ministries:
            st.warning("No ministries indexed yet. Please run the indexing process.")
            st.sidebar.info("To index ministries, run: `python scripts/create_ministry_database.py`")
            st.stop()
            
        selected_ministry = st.sidebar.selectbox(
            "Select Ministry",
            ministries,
            help="Choose a ministry to search within"
        )
        
        # Display ministry statistics
        doc_count = vector_store.get_ministry_document_count(selected_ministry)
        st.sidebar.metric("Documents Indexed", doc_count)
        
        # Main query interface
        user_question = st.text_area(
            "Ask your question:",
            placeholder="e.g., What is the budget allocation for education?",
            height=100
        )
        
        col1, col2 = st.columns([3, 1])
        
        with col1:
            search_button = st.button("Get Answer", type="primary")
        
        with col2:
            show_sources = st.checkbox("Show Sources", value=True)
        
        if search_button and user_question.strip():
            with st.spinner("Searching for relevant information..."):
                try:
                    # Search for relevant documents using PostgreSQL
                    relevant_docs = vector_store.search_by_text(
                        user_question, 
                        selected_ministry, 
                        n_results=5
                    )
                    
                    if relevant_docs:
                        # Generate answer using LLM
                        context = "\n\n".join([doc['text'] for doc in relevant_docs])
                        answer = llm_client.generate_answer(user_question, context)
                        
                        # Display results
                        st.subheader("Answer")
                        st.write(answer)
                        
                        # Show sources if requested
                        if show_sources:
                            with st.expander("View Sources", expanded=False):
                                for i, doc in enumerate(relevant_docs, 1):
                                    relevance = doc.get('relevance_score', 0)
                                    st.write(f"**Source {i}** (Relevance: {relevance:.2f})")
                                    st.write(doc['text'][:500] + "..." if len(doc['text']) > 500 else doc['text'])
                                    
                                    # Show metadata if available
                                    metadata = doc.get('metadata', {})
                                    if metadata:
                                        st.caption(f"Source: {metadata.get('source', 'Unknown')}")
                                    st.write("---")
                    else:
                        st.warning("No relevant information found for your question.")
                        st.info("Try rephrasing your question or selecting a different ministry.")
                        
                except Exception as e:
                    st.error(f"Error processing your question: {str(e)}")
                    logger.error(f"Error in main query processing: {e}")
        
        elif search_button:
            st.warning("Please enter a question.")
        
        # System status in sidebar
        st.sidebar.subheader("System Status")
        st.sidebar.success(f"PostgreSQL Connected")
        st.sidebar.info(f"{len(ministries)} Ministries Available")
        st.sidebar.info(f"{sum(vector_store.get_ministry_document_count(m) for m in ministries)} Total Documents")
        
        # Database info
        with st.sidebar.expander("System Info"):
            st.write(f"**Environment:** {Config.ENVIRONMENT}")
            st.write(f"**Embedding Model:** {Config.EMBEDDING_MODEL}")
            st.write(f"**Database:** PostgreSQL with pgvector")
            st.write(f"**Storage:** Azure Blob Storage")
        
    except Exception as e:
        st.error(f"Failed to initialize application: {str(e)}")
        logger.error(f"Application initialization error: {e}")

if __name__ == "__main__":
    main()

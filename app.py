import streamlit as st
import logging
import base64
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
from src.config import Config
from src.azure_vector_store import AzureVectorStore
from src.document_processor import DocumentProcessor
from src.llm_client import LLMClient
from src.sansad_client import SansadClient


logging.basicConfig(level=Config.get_log_level())
logger = logging.getLogger(__name__)


def is_irrelevant_response(answer_text: str) -> bool:

    irrelevance_indicators = [
        "not relevant to",
        "outside the scope",
        "not related to parliamentary",
        "not within the jurisdiction",
        "unrelated to government",
        "cannot answer this question as it is not relevant",
        "appears to be outside the scope of parliamentary and governmental matters",
        "not relevant to the ministry's affairs",
        "not relevant to the ministry's functions",
    ]

    answer_lower = answer_text.lower()
    return any(indicator in answer_lower for indicator in irrelevance_indicators)


def get_document_sas_url(ministry: str, filename: str) -> str:

    try:

        blob_service_client = BlobServiceClient.from_connection_string(
            Config.AZURE_STORAGE_CONNECTION_STRING
        )

        container_name = "ministrydatastorage"
        blob_name = f"ministries/{ministry}/{filename}"

        blob_client = blob_service_client.get_blob_client(
            container=container_name, blob=blob_name
        )

        try:

            blob_client.get_blob_properties()
        except Exception as e:
            logger.warning(f"Blob not found: {blob_name}")
            return None

        sas_token = generate_blob_sas(
            account_name=blob_service_client.account_name,
            container_name=container_name,
            blob_name=blob_name,
            account_key=blob_service_client.credential.account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.utcnow() + timedelta(hours=1),
        )

        blob_url = f"https://{blob_service_client.account_name}.blob.core.windows.net/{container_name}/{blob_name}?{sas_token}"

        return blob_url

    except Exception as e:
        logger.error(f"Error generating SAS URL for {filename}: {e}")
        return None


def extract_filename_from_metadata(metadata: dict) -> str:

    for key in ["filename", "source", "file", "document_name"]:
        if key in metadata and metadata[key]:
            filename = metadata[key]

            if "/" in filename:
                filename = filename.split("/")[-1]
            return filename

    return "document.pdf"


def main():
    st.set_page_config(
        page_title="Parliamentary Q&A Assistant", page_icon="🏛️", layout="wide"
    )

    st.title("Parliamentary Q&A Assistant")
    st.markdown("Ask questions about parliamentary affairs by ministry")

    if not Config.validate_environment():
        st.error(
            "Missing required environment variables. Please check your configuration."
        )
        st.stop()

    try:

        vector_store = AzureVectorStore()
        llm_client = LLMClient()
        sansad_client = SansadClient()

        st.sidebar.header("Settings")

        ministries = list(vector_store.indexed_ministries)
        if not ministries:
            st.warning("No ministries indexed yet. Please run the indexing process.")
            st.sidebar.info(
                "To index ministries, run: `python scripts/create_ministry_database.py`"
            )
            st.stop()

        selected_ministry = st.sidebar.selectbox(
            "Select Ministry", ministries, help="Choose a ministry to search within"
        )

        user_question = st.text_area(
            "Ask your question:",
            placeholder="e.g., What is the budget allocation for education?",
            height=100,
        )

        search_button = st.button("Get Answer", type="primary")

        if search_button and user_question.strip():
            with st.spinner("Loading, Please wait..."):
                try:

                    relevant_docs = vector_store.search_by_text(
                        user_question, selected_ministry, n_results=5
                    )

                    if relevant_docs:

                        context = "\n\n".join([doc["text"] for doc in relevant_docs])
                        answer = llm_client.generate_answer(
                            user_question, context, selected_ministry
                        )

                        st.subheader("Ministry Response")
                        st.write(answer)

                        is_irrelevant = is_irrelevant_response(answer)

                        if not is_irrelevant:
                            with st.expander("View Source Documents"):
                                for i, doc in enumerate(relevant_docs, 1):
                                    st.markdown(f"**Source {i}**")
                                    st.markdown(doc["text"])

                                    metadata = doc.get("metadata", {})
                                    filename = extract_filename_from_metadata(metadata)

                                    if filename != "document.pdf":
                                        try:

                                            with st.spinner(
                                                f"Generating secure link for {filename}..."
                                            ):
                                                doc_url = get_document_sas_url(
                                                    selected_ministry, filename
                                                )

                                            if doc_url:

                                                try:

                                                    blob_service_client = BlobServiceClient.from_connection_string(
                                                        Config.AZURE_STORAGE_CONNECTION_STRING
                                                    )
                                                    container_name = (
                                                        "ministrydatastorage"
                                                    )
                                                    blob_name = f"ministries/{selected_ministry}/{filename}"
                                                    blob_client = blob_service_client.get_blob_client(
                                                        container=container_name,
                                                        blob=blob_name,
                                                    )

                                                    pdf_bytes = (
                                                        blob_client.download_blob().readall()
                                                    )

                                                    st.markdown(
                                                        f'<a href="{doc_url}" target="_blank">🔗 View PDF in Browser</a>',
                                                        unsafe_allow_html=True,
                                                    )

                                                except Exception as e:
                                                    st.warning(
                                                        f"Could not load PDF for download: {str(e)}"
                                                    )

                                                    st.markdown(
                                                        f'<a href="{doc_url}" target="_blank">🔗 View PDF in Browser</a>',
                                                        unsafe_allow_html=True,
                                                    )
                                            else:
                                                st.warning("PDF file not accessible")

                                        except Exception as e:
                                            st.warning(
                                                f"Could not generate PDF link: {str(e)}"
                                            )
                                    else:
                                        st.caption("Original PDF file not available")

                                    if "date" in metadata:
                                        st.caption(f"📅 Date: {metadata['date']}")
                                    if "session" in metadata:
                                        st.caption(f"🏛️ Session: {metadata['session']}")
                                    if "page" in metadata:
                                        st.caption(f"📖 Page: {metadata['page']}")

                                    if i < len(relevant_docs):
                                        st.markdown("---")

                        else:

                            st.info(
                                "Alert: This question is not relevant to the ministry affairs."
                            )

                    else:
                        st.warning("No relevant information found for your question.")
                        st.info(
                            "Try rephrasing your question or selecting a different ministry."
                        )

                except Exception as e:
                    st.error(f"Error processing your question: {str(e)}")
                    logger.error(f"Error in main query processing: {e}")

        elif search_button:
            st.warning("Please enter a question.")

    except Exception as e:
        st.error(f"Failed to initialize application: {str(e)}")
        logger.error(f"Application initialization error: {e}")


if __name__ == "__main__":
    main()

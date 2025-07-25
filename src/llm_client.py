import logging
import google.generativeai as genai
from typing import List, Dict, Any, Optional
import asyncio
from concurrent.futures import ThreadPoolExecutor
from .config import Config


logger = logging.getLogger(__name__)


class LLMClient:
    def __init__(self):
        try:
            if not Config.GEMINI_API_KEY:
                raise ValueError("GEMINI_API_KEY not found in configuration")

            genai.configure(api_key=Config.GEMINI_API_KEY)

            self.model = genai.GenerativeModel("gemini-2.0-flash-exp")

            self.executor = ThreadPoolExecutor(max_workers=2)

            logger.info("Successfully initialized Gemini LLM client")

        except Exception as e:
            logger.error(f"Error initializing LLM client: {e}")
            raise

    def generate_answer(self, question: str, context: str, ministry: str = None) -> str:
        try:

            context_docs = self._parse_context_string(context)

            if context_docs:
                response = self._generate_structured_response(
                    question, context_docs, ministry
                )
            else:
                response = self._generate_simple_response(question, ministry)

            logger.info(
                f"Generated response for question about {ministry or 'general topic'}"
            )
            return response

        except Exception as e:
            logger.error(f"Error generating answer: {e}")
            return self._get_error_response()

    def generate_response_with_docs(
        self, question: str, context_docs: List[Dict[str, Any]], ministry: str = None
    ) -> str:
        try:
            return self._generate_structured_response(question, context_docs, ministry)
        except Exception as e:
            logger.error(f"Error generating structured response: {e}")
            return self._get_error_response()

    def generate_response_sync(
        self, question: str, context: List[Dict[str, Any]], ministry: str
    ) -> str:
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            response = loop.run_until_complete(
                self.generate_response_async(question, context, ministry)
            )
            loop.close()
            return response
        except Exception as e:
            logger.error(f"Error in synchronous response generation: {e}")
            return self._get_error_response()

    async def generate_response_async(
        self, question: str, context: List[Dict[str, Any]], ministry: str
    ) -> str:
        try:
            prompt = self._construct_enhanced_prompt(question, context, ministry)

            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                self.executor, self._call_gemini_api, prompt
            )

            if not response or not response.text:
                logger.warning("Empty response from Gemini API")
                return "I apologize, but I couldn't generate a meaningful response. Please try rephrasing your question."

            formatted_response = self._format_parliamentary_response(
                response.text, context
            )
            return formatted_response

        except Exception as e:
            logger.error(f"Error in async response generation: {e}")
            return self._get_error_response()

    def _parse_context_string(self, context: str) -> List[Dict[str, Any]]:
        if not context or not context.strip():
            return []

        chunks = context.split("\n\n")

        parsed_docs = []
        for i, chunk in enumerate(chunks):
            if chunk.strip():
                parsed_docs.append(
                    {
                        "text": chunk.strip(),
                        "metadata": {
                            "chunk_index": i,
                            "source": "parliamentary_document",
                        },
                    }
                )

        return parsed_docs

    def _generate_structured_response(
        self, question: str, context_docs: List[Dict[str, Any]], ministry: str = None
    ) -> str:

        try:
            prompt = self._construct_enhanced_prompt(question, context_docs, ministry)

            response = self._call_gemini_api(prompt)

            if not response or not response.text:
                logger.warning("⚠️  Empty response from Gemini API")
                return "I apologize, but I couldn't generate a meaningful response. Please try rephrasing your question."

            formatted_response = self._format_parliamentary_response(
                response.text, context_docs
            )
            return formatted_response

        except Exception as e:
            logger.error(f"Error in structured response generation: {e}")
            return self._get_error_response()

    def _generate_simple_response(self, question: str, ministry: str = None) -> str:

        try:
            simple_prompt = f"""
You are an AI assistant representing the {ministry or 'Indian Parliament'}.


Question: {question}


Please provide a helpful, accurate response based on your knowledge of Indian parliamentary procedures and government policies. 


Keep the response:
- Factual and professional
- Focused on the ministry's functions if a specific ministry is mentioned
- Helpful for understanding parliamentary/government processes


If you cannot provide accurate information, clearly state the limitations.
"""

            response = self._call_gemini_api(simple_prompt)

            if response and response.text:
                return response.text.strip()
            else:
                return "I apologize, but I couldn't generate a response. Please try rephrasing your question."

        except Exception as e:
            logger.error(f"Error in simple response generation: {e}")
            return self._get_error_response()

    def _construct_enhanced_prompt(
        self, question: str, context_docs: List[Dict[str, Any]], ministry: str = None
    ) -> str:

        try:
            context_parts = []

            for i, doc in enumerate(context_docs[:5], 1):  # Use top 5 documents
                text = doc.get("text", "").strip()
                metadata = doc.get("metadata", {})

                date = metadata.get("date", "Unknown date")
                session = metadata.get("session", "4")
                source = metadata.get(
                    "filename", metadata.get("source", "Unknown source")
                )
                ministry_info = metadata.get("ministry", ministry or "Unknown Ministry")
                page = metadata.get("page", "Unknown page")

                if text:
                    context_entry = f"""SOURCE {i}:
Date: {date}
Session: {session}
Source: {source}
Ministry: {ministry_info}
Page: {page}
Content: {text}
"""
                    context_parts.append(context_entry)

            context_text = "\n---\n".join(context_parts)

            prompt = f"""
You are an official representative of the {ministry or 'Indian Parliament'} in the Indian Parliament.


USER QUESTION:
{question}


CONTEXT FROM PARLIAMENTARY RECORDS:
{context_text}


INSTRUCTIONS:
1. RELEVANCE CHECK:
   * Answer only if the question relates to {ministry or 'parliamentary'}'s functions, policies, or responsibilities.
   * If the question is off-topic, respond: "I am unable to answer this question as it is not relevant to the ministry's affairs."


2. USING CONTEXT:
   * Base your answer primarily on the parliamentary records provided in the context.
   * If the context contains relevant information, cite it specifically (e.g., "According to the record from [date/session]...").
   * If the context is insufficient but the question is valid, use your knowledge of Indian government policies and programs.
   * If using general knowledge, clearly state: "Based on general information about the ministry's policies..."


3. ANSWER FORMAT:
   * Begin with a formal answer to the question.
   * Include specific facts, figures, and dates from the context when available.
   * Organize information logically with clear sections.
   * End with any relevant initiatives or future plans mentioned in the context.


4. TONE:
   * Formal and professional
   * Factual and precise
   * Solution-oriented


Generate a comprehensive, accurate response based on these instructions.
Do not answer irrelevant questions like what's the climate, etc.
"""

            return prompt

        except Exception as e:
            logger.error(f"Error constructing enhanced prompt: {e}")
            return f"You are representing {ministry or 'Parliament'}. Answer this question based on the provided context: {question}"

    def _call_gemini_api(self, prompt: str):

        try:
            generation_config = {
                "temperature": 0.7,
                "top_p": 0.8,
                "top_k": 40,
                "max_output_tokens": 3000,
            }

            safety_settings = [
                {
                    "category": "HARM_CATEGORY_HARASSMENT",
                    "threshold": "BLOCK_MEDIUM_AND_ABOVE",
                },
                {
                    "category": "HARM_CATEGORY_HATE_SPEECH",
                    "threshold": "BLOCK_MEDIUM_AND_ABOVE",
                },
                {
                    "category": "HARM_CATEGORY_SEXUALLY_EXPLICIT",
                    "threshold": "BLOCK_MEDIUM_AND_ABOVE",
                },
                {
                    "category": "HARM_CATEGORY_DANGEROUS_CONTENT",
                    "threshold": "BLOCK_MEDIUM_AND_ABOVE",
                },
            ]

            return self.model.generate_content(
                prompt,
                generation_config=generation_config,
                safety_settings=safety_settings,
            )

        except Exception as e:
            logger.error(f"Error calling Gemini API: {e}")
            raise

    def _format_parliamentary_response(
        self, text: str, context_docs: List[Dict[str, Any]]
    ) -> str:

        try:
            formatted_text = text.strip()

            if self._is_irrelevant_response(formatted_text):
                return "I apologize, but your question appears to be outside the scope of parliamentary and governmental matters. Please ask questions related to government policies, parliamentary procedures, or ministry functions."

            if context_docs and not self._is_irrelevant_response(formatted_text):
                meaningful_citations = []

                for i, doc in enumerate(
                    context_docs[:3], 1
                ):  # Top 3 sources for citations
                    metadata = doc.get("metadata", {})
                    date = metadata.get("date", "Unknown date")
                    session = metadata.get("session", "Unknown session")
                    source = metadata.get(
                        "filename", metadata.get("source", "Unknown source")
                    )

                    if (
                        date != "Unknown date"
                        or session
                        not in ["Unknown session", "4"]  # "4" is also a generic default
                        or source not in ["Unknown source", "parliamentary_document"]
                    ):

                        meaningful_citations.append(
                            f"[{i}] Parliamentary record from Session {session}, dated {date} (Source: {source})"
                        )

                if meaningful_citations:
                    formatted_text += f"\n\n**Sources:**\n" + "\n".join(
                        meaningful_citations
                    )

            return formatted_text

        except Exception as e:
            logger.error(f"Error formatting response: {e}")
            return text

    def _is_irrelevant_response(self, text: str) -> bool:

        irrelevance_indicators = [
            "unable to answer this question as it is not relevant to the ministry's affairs",
            "not relevant to the ministry's functions",
            "does not fall under the purview of this ministry",
            "outside the scope of this ministry",
            "not within the jurisdiction of this ministry",
            "not relevant to",
            "outside the scope",
            "not related to parliamentary",
            "not within the jurisdiction",
            "unrelated to government",
            "cannot answer this question as it is not relevant",
        ]

        text_lower = text.lower()
        return any(indicator in text_lower for indicator in irrelevance_indicators)

    def _get_error_response(self) -> str:

        return (
            "I apologize, but I encountered an error while processing your question. "
            "This might be due to connection issues or service limitations. "
            "Please try rephrasing your question or wait a moment before retrying."
        )

    def test_connection(self) -> bool:

        try:
            test_response = self._call_gemini_api(
                "Respond with 'Connection successful' if you can process this message."
            )

            if test_response and test_response.text:
                logger.info("LLM connection test successful")
                return True
            else:
                logger.error("LLM connection test failed - no response")
                return False

        except Exception as e:
            logger.error(f"LLM connection test failed: {e}")
            return False

    def __del__(self):

        try:
            if hasattr(self, "executor"):
                self.executor.shutdown(wait=True)
        except Exception as e:
            logger.error(f"Error shutting down LLM client executor: {e}")

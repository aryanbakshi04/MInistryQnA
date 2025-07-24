import sys
import os
import logging
from pathlib import Path
import difflib
import re
from pypdf import PdfReader

# Add src to path
sys.path.append(str(Path(__file__).parent.parent))

# run this code when you see this.....!!!!!!

from src.config import Config
from src.azure_vector_store import AzureVectorStore
from src.azure_storage import AzureBlobStorage
from src.document_processor import DocumentProcessor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ContentBasedPDFProcessor:
    def __init__(self):
        self.vector_store = AzureVectorStore()
        self.storage = AzureBlobStorage()
        self.document_processor = DocumentProcessor()
        self.pdf_cache_dir = Path("data/pdf_cache")
        
        # Complete list of ministries
        self.ministries = [
            "Ministry of Steel",
            "Ministry of Chemicals and Fertilizers",
            "Ministry of Fisheries, Animal Husbandry and Dairying",
            "Ministry of Law and Justice",
            "Ministry of Corporate Affairs",
            "Ministry of Road Transport and Highways",
            "Ministry of Tourism",
            "Ministry of Women and Child Development",
            "Ministry of Culture",
            "Ministry of Labour and Employment",
            "Ministry of Consumer Affairs, Food and Public Distribution",
            "Ministry of Panchayati Raj",
            "Ministry of Coal",
            "Ministry of Commerce and Industry",
            "Ministry of Housing and Urban Affairs",
            "Ministry of Environment, Forest and Climate Change",
            "Ministry of Micro, Small and Medium Enterprises",
            "Ministry of Rural Development",
            "Ministry of Parliamentary Affairs",
            "Ministry of Electronics and Information Technology",
            "Ministry of Agriculture and Farmers Welfare",
            "Ministry of Communications",
            "Ministry of Petroleum and Natural Gas",
            "Ministry of External Affairs",
            "Ministry of Power",
            "Ministry of Heavy Industries",
            "Ministry of Personnel, Public Grievances and Pensions",
            "Ministry of Statistics and Programme Implementation",
            "Ministry of Textiles",
            "NITI Aayog",
            "Ministry of Science and Technology",
            "Ministry of Mines",
            "Ministry of Earth Sciences",
            "Prime Minister's Office",
            "Ministry of Youth Affairs and Sports",
            "Ministry of Education",
            "Ministry of Jal Shakti",
            "Ministry of Tribal Affairs",
            "Ministry of Railways",
            "Ministry of Civil Aviation",
            "Ministry of Finance",
            "Ministry of Development of North Eastern Region",
            "Ministry of Food Processing Industries",
            "Ministry of Defence",
            "Ministry of Health and Family Welfare",
            "Ministry of Social Justice and Empowerment",
            "Ministry of Information and Broadcasting",
            "Ministry of Minority Affairs",
            "Ministry of Ports, Shipping and Waterways",
            "Ministry of Home Affairs",
            "Ministry of New and Renewable Energy",
            "Ministry of Skill Development and Entrepreneurship"
        ]
        
        # Create enhanced keyword mapping
        self.ministry_keywords = self._create_comprehensive_keyword_mapping()
        
        # Cache for processed PDFs to avoid re-reading
        self.content_cache = {}
    
    def _create_comprehensive_keyword_mapping(self):
        """Create comprehensive keyword mapping including variations and abbreviations"""
        keyword_map = {}
        
        for ministry in self.ministries:
            # Extract words from ministry name
            ministry_clean = ministry.lower()
            words = re.findall(r'\b\w+\b', ministry_clean)
            
            # Add each significant word
            for word in words:
                if len(word) > 2 and word not in ['of', 'and', 'the', 'for']:
                    if word not in keyword_map:
                        keyword_map[word] = []
                    keyword_map[word].append(ministry)
            
            # Add common abbreviations and variations
            abbreviations = self._get_ministry_abbreviations(ministry)
            for abbrev in abbreviations:
                if abbrev not in keyword_map:
                    keyword_map[abbrev] = []
                keyword_map[abbrev].append(ministry)
        
        return keyword_map
    
    def _get_ministry_abbreviations(self, ministry):
        """Get common abbreviations and variations for ministries"""
        abbreviations = []
        
        # Common abbreviations mapping
        abbrev_map = {
            "Ministry of Defence": ["mod", "defence", "defense"],
            "Ministry of Finance": ["mof", "finance", "finmin"],
            "Ministry of Education": ["moe", "education", "shiksha"],
            "Ministry of Health and Family Welfare": ["mohfw", "health", "family welfare"],
            "Ministry of Home Affairs": ["mha", "home affairs", "home"],
            "Ministry of External Affairs": ["mea", "external affairs", "foreign"],
            "Ministry of Railways": ["railway", "rail", "indian railways"],
            "Ministry of Agriculture and Farmers Welfare": ["agriculture", "farmers", "krishi"],
            "Ministry of Information and Broadcasting": ["i&b", "information", "broadcasting"],
            "Prime Minister's Office": ["pmo", "prime minister"],
            "NITI Aayog": ["niti", "aayog", "planning commission"],
            "Ministry of Road Transport and Highways": ["morth", "transport", "highways"],
            "Ministry of Power": ["power", "energy", "electricity"],
            "Ministry of Coal": ["coal", "mines"],
            "Ministry of Steel": ["steel", "iron"],
            "Ministry of Textiles": ["textiles", "textile", "handloom"],
            "Ministry of Tourism": ["tourism", "tourist"],
            "Ministry of Labour and Employment": ["labour", "employment", "shram"],
            "Ministry of Science and Technology": ["science", "technology", "dst"],
            "Ministry of Environment, Forest and Climate Change": ["environment", "forest", "climate", "moefcc"],
        }
        
        return abbrev_map.get(ministry, [])
    
    def extract_first_page_text(self, pdf_path):
        """Extract text from the first page of PDF efficiently"""
        try:
            # Check cache first
            if pdf_path in self.content_cache:
                return self.content_cache[pdf_path]
            
            with open(pdf_path, 'rb') as file:
                pdf_reader = PdfReader(file)
                
                if len(pdf_reader.pages) == 0:
                    logger.warning(f"No pages found in {pdf_path}")
                    return ""
                
                # Extract text from first page only
                first_page = pdf_reader.pages[0]
                text = first_page.extract_text()
                
                # Clean and normalize text
                text = re.sub(r'\s+', ' ', text.strip())
                text = text.lower()
                
                # Cache the result
                self.content_cache[pdf_path] = text
                
                return text
                
        except Exception as e:
            logger.error(f"Error extracting text from {pdf_path}: {e}")
            return ""
    
    def get_ministry_from_content(self, pdf_path):
        """Extract ministry name from PDF first page content"""
        logger.info(f"🔍 Analyzing content of: {Path(pdf_path).name}")
        
        # Extract first page text
        content = self.extract_first_page_text(pdf_path)
        
        if not content:
            logger.warning(f"No content extracted from {Path(pdf_path).name}")
            return "Ministry of Parliamentary Affairs"  # Default fallback
        
        # Method 1: Direct ministry name search
        for ministry in self.ministries:
            ministry_lower = ministry.lower()
            # Remove "ministry of" for better matching
            ministry_clean = ministry_lower.replace("ministry of ", "")
            
            if ministry_lower in content or ministry_clean in content:
                logger.info(f"✅ Direct match found: {ministry}")
                return ministry
        
        # Method 2: Keyword-based matching
        matched_ministries = []
        match_scores = {}
        
        for keyword, ministries in self.ministry_keywords.items():
            if keyword in content:
                for ministry in ministries:
                    matched_ministries.append(ministry)
                    match_scores[ministry] = match_scores.get(ministry, 0) + 1
        
        if matched_ministries:
            # Return ministry with highest keyword matches
            best_match = max(match_scores, key=match_scores.get)
            logger.info(f"✅ Keyword match found: {best_match} (score: {match_scores[best_match]})")
            return best_match
        
        # Method 3: Fuzzy string matching with ministry names
        best_match = None
        best_ratio = 0
        
        for ministry in self.ministries:
            ministry_clean = ministry.replace("Ministry of ", "").lower()
            
            # Check similarity with content snippets
            content_words = content.split()[:50]  # Check first 50 words
            content_snippet = " ".join(content_words)
            
            ratio = difflib.SequenceMatcher(None, content_snippet, ministry_clean).ratio()
            
            if ratio > best_ratio and ratio > 0.15:  # Lower threshold for content matching
                best_ratio = ratio
                best_match = ministry
        
        if best_match:
            logger.info(f"✅ Fuzzy match found: {best_match} (similarity: {best_ratio:.2f})")
            return best_match
        
        # Method 4: Pattern-based detection (common PDF headers)
        patterns = [
            r'government of india\s+(.+?)(?:\n|ministry)',
            r'भारत सरकार\s+(.+?)(?:\n|मंत्रालय)',
            r'ministry of ([a-z\s,&]+)',
            r'मंत्रालय\s+(.+?)(?:\n|government)',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            if matches:
                extracted_text = matches[0].strip()
                # Try to match extracted text with ministry names
                for ministry in self.ministries:
                    if extracted_text.lower() in ministry.lower() or ministry.lower() in extracted_text.lower():
                        logger.info(f"✅ Pattern match found: {ministry}")
                        return ministry
        
        logger.warning(f"⚠️  No ministry match found for {Path(pdf_path).name}, using default")
        logger.debug(f"Content preview: {content[:200]}...")
        return "Ministry of Parliamentary Affairs"  # Default fallback
    
    def batch_analyze_pdfs(self, pdf_files, show_preview=True):
        """Analyze all PDFs and show ministry assignments"""
        assignments = {}
        
        logger.info(f"🔍 Analyzing {len(pdf_files)} PDF files for ministry classification...")
        
        for i, pdf_file in enumerate(pdf_files, 1):
            logger.info(f"\n--- Analyzing {i}/{len(pdf_files)}: {pdf_file.name} ---")
            
            ministry = self.get_ministry_from_content(str(pdf_file))
            assignments[pdf_file] = ministry
            
            if show_preview:
                # Show first few lines of content for verification
                content = self.extract_first_page_text(str(pdf_file))
                preview = content[:150] + "..." if len(content) > 150 else content
                logger.info(f"📄 Content preview: {preview}")
                logger.info(f"🏛️  Assigned to: {ministry}")
        
        return assignments
    
    def interactive_review_assignments(self, assignments):
        """Allow user to review and correct assignments"""
        print(f"\n📋 Review Ministry Assignments")
        print("=" * 50)
        
        corrected_assignments = {}
        
        for i, (pdf_file, ministry) in enumerate(assignments.items(), 1):
            print(f"\n--- File {i}/{len(assignments)}: {pdf_file.name} ---")
            print(f"🤖 Auto-assigned to: {ministry}")
            
            # Show content preview
            content = self.extract_first_page_text(str(pdf_file))
            preview = content[:200] + "..." if len(content) > 200 else content
            print(f"📄 Content: {preview}")
            
            choice = input("Accept assignment? (y/n/list/skip): ").strip().lower()
            
            if choice == 'n':
                self.show_ministry_list()
                new_choice = input("Enter ministry number (1-51): ").strip()
                if new_choice.isdigit() and 1 <= int(new_choice) <= len(self.ministries):
                    corrected_assignments[pdf_file] = self.ministries[int(new_choice) - 1]
                    print(f"✅ Reassigned to: {self.ministries[int(new_choice) - 1]}")
                else:
                    corrected_assignments[pdf_file] = ministry
                    print("❌ Invalid choice, keeping original assignment")
            elif choice == 'list':
                self.show_ministry_list()
                corrected_assignments[pdf_file] = ministry
            elif choice == 'skip':
                continue
            else:
                corrected_assignments[pdf_file] = ministry
                print("✅ Assignment accepted")
        
        return corrected_assignments
    
    def show_ministry_list(self):
        """Display numbered list of ministries"""
        print("\n📋 Available Ministries:")
        for i, ministry in enumerate(self.ministries, 1):
            print(f"{i:2d}. {ministry}")
        print()
    
    def process_all_pdfs(self, interactive=True):
        """Process all PDFs with content-based ministry assignment"""
        if not self.pdf_cache_dir.exists():
            logger.error(f"PDF cache directory not found: {self.pdf_cache_dir}")
            return False
        
        # Get all PDF files
        pdf_files = list(self.pdf_cache_dir.glob("*.pdf"))
        
        if not pdf_files:
            logger.warning(f"No PDF files found in {self.pdf_cache_dir}")
            return False
        
        logger.info(f"Found {len(pdf_files)} PDF files to process")
        
        # Analyze PDF content for ministry assignment
        assignments = self.batch_analyze_pdfs(pdf_files)
        
        # Interactive review if requested
        if interactive:
            assignments = self.interactive_review_assignments(assignments)
        
        if not assignments:
            logger.warning("No files assigned for processing")
            return False
        
        # Group by ministry
        ministry_groups = {}
        for pdf_file, ministry in assignments.items():
            if ministry not in ministry_groups:
                ministry_groups[ministry] = []
            ministry_groups[ministry].append(pdf_file)
        
        logger.info(f"\n📊 Final Processing Summary:")
        for ministry, files in ministry_groups.items():
            logger.info(f"  {ministry}: {len(files)} files")
        
        # Process each ministry group
        total_documents = 0
        successful_ministries = 0
        
        for ministry, pdf_files in ministry_groups.items():
            logger.info(f"\n🔄 Processing {ministry}...")
            
            if self.vector_store.is_ministry_indexed(ministry):
                logger.info(f"⚠️  {ministry} is already indexed. Skipping...")
                continue
            
            ministry_documents = []
            
            for pdf_file in pdf_files:
                try:
                    logger.info(f"📄 Processing: {pdf_file.name}")
                    
                    # Process PDF for document extraction
                    documents = self.document_processor.process_local_pdf(str(pdf_file), ministry)
                    
                    if documents:
                        ministry_documents.extend(documents)
                        logger.info(f"✅ Extracted {len(documents)} chunks")
                        
                        # Upload to Azure Blob Storage
                        blob_name = f"ministries/{ministry}/{pdf_file.name}"
                        blob_url = self.storage.upload_pdf(str(pdf_file), blob_name)
                        if blob_url:
                            logger.info(f"☁️  Uploaded to blob storage")
                    else:
                        logger.warning(f"⚠️  No content extracted")
                        
                except Exception as e:
                    logger.error(f"❌ Error processing {pdf_file.name}: {e}")
                    continue
            
            # Add documents to vector store
            if ministry_documents:
                logger.info(f"💾 Adding {len(ministry_documents)} documents to vector store")
                self.vector_store.add_documents(ministry_documents, ministry)
                total_documents += len(ministry_documents)
                successful_ministries += 1
                logger.info(f"✅ Successfully indexed {ministry}")
            else:
                logger.warning(f"⚠️  No documents to index for {ministry}")
        
        # Final summary
        logger.info(f"\n🎉 Processing Completed!")
        logger.info(f"✅ Total documents indexed: {total_documents}")
        logger.info(f"✅ Ministries successfully indexed: {successful_ministries}")
        
        return successful_ministries > 0

def main():
    """Main function"""
    processor = ContentBasedPDFProcessor()
    
    print("🏛️  Parliamentary Q&A - Content-Based PDF Processing")
    print("=" * 60)
    
    # Check if PDFs exist
    pdf_files = list(Path("data/pdf_cache").glob("*.pdf"))
    if not pdf_files:
        print("❌ No PDF files found in data/pdf_cache/")
        return
    
    print(f"📄 Found {len(pdf_files)} PDF files")
    print("📋 This script will read the first page of each PDF to determine the ministry")
    
    # Processing mode
    print("\nChoose processing mode:")
    print("1. Interactive mode (review assignments)")
    print("2. Auto mode (trust AI assignments)")
    
    choice = input("Enter your choice (1 or 2): ").strip()
    interactive = choice == "1"
    
    # Process PDFs
    success = processor.process_all_pdfs(interactive=interactive)
    
    if success:
        logger.info("\n🎉 Content-based PDF processing completed!")
        logger.info("🚀 You can now run: streamlit run app.py")
    else:
        logger.error("\n❌ PDF processing failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()

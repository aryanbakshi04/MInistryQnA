import os
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)

class Config:
    # Database Configuration
    POSTGRESQL_URL = os.getenv("POSTGRESQL_URL")
    
    # API Keys
    GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
    
    # Azure Storage
    AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    AZURE_STORAGE_CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "MinistryQnA")
    
    # Application Settings
    ENVIRONMENT = os.getenv("ENVIRONMENT", "development")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    
    # Embedding Model
    EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
    
    # Sansad API Configuration (from your existing sansad_client.py)
    SANSAD_API_URL = "https://sansad.in/qetFile/loksabhaquestions"
    PDF_BASE_URL = "https://sansad.in"
    PDF_CACHE_DIR = "data/pdf_cache"
    
    # API Parameters  
    DEFAULT_LOK_SABHA = "17"
    DEFAULT_SESSION = "1"
    DEFAULT_PAGE_SIZE = 20
    
    # Request Configuration
    MAX_RETRIES = 3
    RATE_LIMIT_DELAY = 1.0
    TIMEOUT = 30
    
    MINISTRIES = [
        "Ministry of Agriculture and Farmers Welfare",
        "Ministry of Chemicals and Fertilizers",
        "Ministry of Civil Aviation",
        "Ministry of Coal",
        "Ministry of Commerce and Industry",
        "Ministry of Communications",
        "Ministry of Consumer Affairs, Food and Public Distribution",
        "Ministry of Corporate Affairs",
        "Ministry of Culture",
        "Ministry of Defence",
        "Ministry of Development of North Eastern Region",
        "Ministry of Earth Sciences",
        "Ministry of Education",
        "Ministry of Electronics and Information Technology",
        "Ministry of Environment, Forest and Climate Change",
        "Ministry of External Affairs",
        "Ministry of Finance",
        "Ministry of Fisheries, Animal Husbandry and Dairying",
        "Ministry of Food Processing Industries",
        "Ministry of Health and Family Welfare",
        "Ministry of Heavy Industries",
        "Ministry of Home Affairs",
        "Ministry of Housing and Urban Affairs",
        "Ministry of Information and Broadcasting",
        "Ministry of Jal Shakti",
        "Ministry of Labour and Employment",
        "Ministry of Law and Justice",
        "Ministry of Micro, Small and Medium Enterprises",
        "Ministry of Mines",
        "Ministry of Minority Affairs",
        "Ministry of New and Renewable Energy",
        "Ministry of Panchayati Raj",
        "Ministry of Parliamentary Affairs",
        "Ministry of Personnel, Public Grievances and Pensions",
        "Ministry of Petroleum and Natural Gas",
        "Ministry of Power",
        "Ministry of Railways",
        "Ministry of Road Transport and Highways",
        "Ministry of Rural Development",
        "Ministry of Science and Technology",
        "Ministry of Ports, Shipping and Waterways",
        "Ministry of Skill Development and Entrepreneurship",
        "Ministry of Social Justice and Empowerment",
        "Ministry of Statistics and Programme Implementation",
        "Ministry of Steel",
        "Ministry of Textiles",
        "Ministry of Tourism",
        "Ministry of Tribal Affairs",
        "Ministry of Women and Child Development",
        "Ministry of Youth Affairs and Sports",
        "Prime Minister's Office",
        "NITI Aayog",
    ]

    @classmethod
    def validate_environment(cls):
        """Validate that all required environment variables are set"""
        required_vars = [
            ("POSTGRESQL_URL", cls.POSTGRESQL_URL),
            ("GEMINI_API_KEY", cls.GEMINI_API_KEY)
        ]
        
        missing_vars = []
        for var_name, var_value in required_vars:
            if not var_value:
                missing_vars.append(var_name)
        
        if missing_vars:
            logger.error(f"Missing required environment variables: {missing_vars}")
            return False
        
        return True
    
    @classmethod
    def get_log_level(cls):
        """Get logging level from configuration"""
        level_map = {
            'DEBUG': logging.DEBUG,
            'INFO': logging.INFO,
            'WARNING': logging.WARNING,
            'ERROR': logging.ERROR
        }
        return level_map.get(cls.LOG_LEVEL.upper(), logging.INFO)

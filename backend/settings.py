# File: settings.py

import os
from pathlib import Path

# Base directory of the project
BASE_DIR = Path(__file__).resolve().parent

class APIConfig:
    OPENALEX_SUBFIELDS_BASE = "https://api.openalex.org/subfields"
    USER_AGENT = "ScienceNgramsWebapp/1.0"
    MAX_PER_PAGE = 200
    REQUEST_TIMEOUT = 30  # seconds

class CacheFiles:
    CACHE_ROOT = BASE_DIR / "cache"
    SUBFIELDS_CACHE_PATH = CACHE_ROOT / "subfield_hierarchy.json"

class RawDataFiles:
    RAW_DATASET_DIR = BASE_DIR / "data"
    RAW_FILE_PATTERN = "-perSubfield.txt"

class DatabaseConfig:
    POSTGRES_URL = os.getenv(
        "DATABASE_URL", 
        "postgresql://science_ngram_user:science_ngram_password@localhost:5432/science_ngram_db"
    )
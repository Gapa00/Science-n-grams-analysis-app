# app/core/config.py
from pydantic_settings import BaseSettings
from pydantic import Extra

class Settings(BaseSettings):
    DATABASE_URL: str

    class Config:
        extra = Extra.allow  # ✅ allow additional fields from .env
        env_file = ".env"

settings = Settings()

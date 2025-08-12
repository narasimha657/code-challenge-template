from pydantic_settings import BaseSettings
from typing import List
import secrets
import os

class Settings(BaseSettings):
    PROJECT_NAME: str = "corteva App API"
    API_V1_STR: str = "/api/v1"
    DEBUG: bool = os.getenv("DEBUG", True)
    DB_NAME: str = os.getenv("DB_NAME", "postgres")
    DB_USER: str = os.getenv("DB_USER", "postgres")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD", "root")
    DB_HOST: str = os.getenv("DB_HOST", "localhost")
    DB_PORT: str = os.getenv("DB_PORT", "5437") 
    
    class Config:
        env_file = ".env"
        env_file_encoding = 'utf-8'
        case_sensitive = True

    @property
    def get_database_uri(self) -> str:
        return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"


settings = Settings()
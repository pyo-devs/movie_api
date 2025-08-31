from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict

# 변수 세팅
BASE_DIR = Path(__file__).resolve().parents[1]
class Settings(BaseSettings):
    TMDB_BASE:str
    TMDB_API_KEY:str
    TMDB_API_TOKEN:str
    DATABASE_URL:str
    model_config = SettingsConfigDict(env_file=BASE_DIR / ".env", env_file_encoding="utf-8")

settings = Settings()
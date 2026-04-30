from pydantic import BaseModel
from pydantic_settings import (
    BaseSettings,
    SettingsConfigDict,
)

class YandexSettings(BaseModel):
    FOLDER_ID: str
    AUTH: str
    ASSISTANT_INSTRUCTION: str
    METADATA_INSTRUCTION: str
    URL: str
    MODEL: str
    GET_INFO_MCP_URL:str
    LOG_FILE_NAME: str
    PORT: int
    DATA_INSTRUCTION: str
    WAIT_TIMEOUT: int
    METADATA_SCHEMA: str = "public"
    MAX_TURNS: int = 25
    SESSION_MAX_HISTORY: int = 0
    SQL_PRESETS: list[dict[str, str]] = []
    DEBUG: bool = False

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        case_sensitive=False,
        env_nested_delimiter="__",
    )
    yandex: YandexSettings

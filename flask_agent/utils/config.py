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
    SECRET_KEY: str
    PORT: int
    MASKING_INSTRUCTION: str
    MASKING_INDEX_ID: str 

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        case_sensitive=False,
        env_nested_delimiter="__",
    )
    yandex: YandexSettings

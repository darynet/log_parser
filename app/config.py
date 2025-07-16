from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "FastAPI Kafka Consumer"
    app_version: str = "1.0.0"

    model_config: SettingsConfigDict = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="allow",
        case_sensitive=False,
        env_prefix="",
        validate_default=True,
    )


settings: Settings = Settings()

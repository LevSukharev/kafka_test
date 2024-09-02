from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=("../.env.dev", "../src/.env.secret"),
        env_file_encoding="utf-8",
    )

    host: str
    db: str
    user: str
    password: str
    port: int
    user_kafka: str
    password_kafka: str
    topic: str
    bootstrap_servers: str

settings = Settings()
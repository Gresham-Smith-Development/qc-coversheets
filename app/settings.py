from functools import lru_cache

from pydantic import AliasChoices
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = Field(default="postgresql://postgres:postgres@localhost:5432/postgres")

    erp_hmac_secret: str = Field(default="")
    erp_fetch_base_url: str = Field(default="https://vp.greshamsmith.com/vantagepoint/api")

    erp_token_url: str = Field(default="")
    erp_client_id: str = Field(default="", validation_alias=AliasChoices("ERP_CLIENT_ID", "Client_Id"))
    erp_client_secret: str = Field(default="")
    erp_scope: str = Field(default="")
    erp_stored_procedure: str = Field(default="")
    erp_username: str = Field(default="", validation_alias=AliasChoices("ERP_USERNAME", "Username"))
    erp_password: str = Field(default="", validation_alias=AliasChoices("ERP_PASSWORD", "Password"))
    erp_grant_type: str = Field(default="password")
    erp_integrated: str = Field(default="N")
    erp_database: str = Field(default="Vantagepoint")
    erp_refresh_token: str = Field(default="")

    ingest_concurrency_limit: int = Field(default=20)
    ingest_mode: str = Field(default="sync")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()

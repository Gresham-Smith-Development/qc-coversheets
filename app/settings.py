from functools import lru_cache
from uuid import UUID

from pydantic import AliasChoices
from pydantic import Field
from pydantic import field_validator
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
    test_mode: bool = Field(default=False)
    environment_name: str = Field(default="local")

    auth_entra_authority: str = Field(default="https://login.microsoftonline.com")
    auth_entra_tenant_id: str = Field(default="")
    auth_entra_client_id: str = Field(default="")
    auth_entra_client_secret: str = Field(default="")
    auth_scope: str = Field(default="openid profile email")

    auth_redirect_uri_local: str = Field(default="http://localhost:8000/auth/callback")
    auth_redirect_uri_test: str = Field(default="https://test.example.com/auth/callback")
    auth_redirect_uri_prod: str = Field(default="https://app.example.com/auth/callback")
    auth_logout_redirect_uri_local: str = Field(default="http://localhost:8000/dev/admin")
    auth_logout_redirect_uri_test: str = Field(default="https://test.example.com")
    auth_logout_redirect_uri_prod: str = Field(default="https://app.example.com")
    auth_login_success_redirect: str = Field(default="/dev/admin")
    auth_sync_entra_app_roles: bool = Field(default=True)

    session_secret: str = Field(default="change-me-session-secret")
    session_cookie_name: str = Field(default="qc_session")
    session_https_only: bool = Field(default=False)
    session_same_site: str = Field(default="lax")
    session_max_age_seconds: int = Field(default=28800)

    auth_bypass_enabled: bool = Field(default=False)
    auth_bypass_tenant_id: str = Field(default="local-tenant")
    auth_bypass_object_id: str = Field(default="local-object")
    auth_bypass_email: str = Field(default="local.admin@example.com")
    auth_bypass_display_name: str = Field(default="Local Admin")
    auth_bypass_app_user_id: UUID = Field(default=UUID("00000000-0000-0000-0000-000000000001"))

    auth_admin_bootstrap_allowlist_object_ids: str = Field(default="")

    def selected_redirect_uri(self) -> str:
        env = self.environment_name.lower()
        if env in {"local", "development"}:
            return self.auth_redirect_uri_local
        if env in {"test", "staging", "qa"}:
            return self.auth_redirect_uri_test
        return self.auth_redirect_uri_prod

    def selected_logout_redirect_uri(self) -> str:
        env = self.environment_name.lower()
        if env in {"local", "development"}:
            return self.auth_logout_redirect_uri_local
        if env in {"test", "staging", "qa"}:
            return self.auth_logout_redirect_uri_test
        return self.auth_logout_redirect_uri_prod

    def admin_bootstrap_allowlist(self) -> set[str]:
        raw = self.auth_admin_bootstrap_allowlist_object_ids
        return {item.strip().lower() for item in raw.split(",") if item.strip()}

    @field_validator("auth_bypass_app_user_id", mode="before")
    @classmethod
    def coerce_blank_bypass_uuid(cls, value: object) -> object:
        if value is None:
            return UUID("00000000-0000-0000-0000-000000000001")
        if isinstance(value, str) and not value.strip():
            return UUID("00000000-0000-0000-0000-000000000001")
        return value


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()

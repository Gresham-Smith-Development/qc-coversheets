from app.settings import Settings


def test_redirect_uri_selection_local() -> None:
    settings = Settings(
        environment_name="local",
        auth_redirect_uri_local="http://localhost:8000/auth/callback",
        auth_redirect_uri_test="https://test.example.com/auth/callback",
        auth_redirect_uri_prod="https://prod.example.com/auth/callback",
    )
    assert settings.selected_redirect_uri() == "http://localhost:8000/auth/callback"


def test_admin_bootstrap_allowlist_parsing() -> None:
    settings = Settings(
        auth_admin_bootstrap_allowlist_object_ids="A,B, c ",
    )
    assert settings.admin_bootstrap_allowlist() == {"a", "b", "c"}

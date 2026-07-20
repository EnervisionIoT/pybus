from pybus.container.config import ApplicationSettings


def test_default_application_settings():
    settings = ApplicationSettings(_env_file=None)

    assert settings.APPLICATION_NAME == "pybus"
    assert settings.DATABASE_TYPE == "sqlalchemy"
    assert settings.POSTGRES_SERVER == "postgres"
    assert settings.POSTGRES_PORT == 5432


def test_sqlalchemy_database_uri_is_built_from_postgres_fields():
    settings = ApplicationSettings(
        _env_file=None,
        POSTGRES_USER="user",
        POSTGRES_PASSWORD="pass",
        POSTGRES_SERVER="db-host",
        POSTGRES_PORT=5433,
        POSTGRES_DB="mydb",
    )

    uri = str(settings.SQLALCHEMY_DATABASE_URI)

    assert uri.startswith("postgresql+psycopg://")
    assert "user:pass" in uri
    assert "db-host:5433" in uri
    assert uri.endswith("/mydb")

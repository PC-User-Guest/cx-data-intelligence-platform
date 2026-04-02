from __future__ import annotations

import json
import os


def _clean_env_value(value: str | None) -> str | None:
    if value is None:
        return None

    cleaned = value.strip()
    if cleaned.startswith('"') and cleaned.endswith('"') and len(cleaned) >= 2:
        cleaned = cleaned[1:-1]
    return cleaned


def configure_dlt_bigquery_env() -> None:
    """Map generic GCP environment variables to dlt BigQuery settings."""

    project_id = _clean_env_value(os.getenv("GOOGLE_CLOUD_PROJECT_ID"))
    private_key = _clean_env_value(os.getenv("GOOGLE_CLOUD_PRIVATE_KEY"))
    private_key_id = _clean_env_value(os.getenv("GOOGLE_CLOUD_PRIVATE_KEY_ID"))
    client_email = _clean_env_value(os.getenv("GOOGLE_CLOUD_CLIENT_EMAIL"))
    client_id = _clean_env_value(os.getenv("GOOGLE_CLOUD_CLIENT_ID"))
    auth_uri = _clean_env_value(os.getenv("GOOGLE_CLOUD_AUTH_URI"))
    token_uri = _clean_env_value(os.getenv("GOOGLE_CLOUD_TOKEN_URI"))
    auth_provider_cert_url = _clean_env_value(os.getenv("GOOGLE_CLOUD_AUTH_PROVIDER_X509_CERT_URL"))
    client_cert_url = _clean_env_value(os.getenv("GOOGLE_CLOUD_CLIENT_X509_CERT_URL"))
    bigquery_location = _clean_env_value(os.getenv("BIGQUERY_LOCATION"))

    if project_id:
        os.environ["DESTINATION__BIGQUERY__PROJECT_ID"] = project_id

    if bigquery_location:
        os.environ["DESTINATION__BIGQUERY__LOCATION"] = bigquery_location

    required = [
        project_id,
        private_key,
        private_key_id,
        client_email,
        client_id,
        auth_uri,
        token_uri,
        auth_provider_cert_url,
        client_cert_url,
    ]
    if any(field is None for field in required):
        return

    credentials = {
        "type": "service_account",
        "project_id": project_id,
        "private_key_id": private_key_id,
        "private_key": private_key.replace("\\n", "\n"),
        "client_email": client_email,
        "client_id": client_id,
        "auth_uri": auth_uri,
        "token_uri": token_uri,
        "auth_provider_x509_cert_url": auth_provider_cert_url,
        "client_x509_cert_url": client_cert_url,
    }
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS"] = json.dumps(credentials)
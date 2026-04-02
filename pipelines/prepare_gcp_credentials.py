from __future__ import annotations

import json
import os
from pathlib import Path


def _get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value.strip('"')


def prepare_credentials_file() -> str:
    existing_adc = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if existing_adc and Path(existing_adc).exists():
        return existing_adc

    project_id = _get_required_env("GOOGLE_CLOUD_PROJECT_ID")
    credentials = {
        "type": "service_account",
        "project_id": project_id,
        "private_key_id": _get_required_env("GOOGLE_CLOUD_PRIVATE_KEY_ID"),
        "private_key": _get_required_env("GOOGLE_CLOUD_PRIVATE_KEY").replace("\\n", "\n"),
        "client_email": _get_required_env("GOOGLE_CLOUD_CLIENT_EMAIL"),
        "client_id": _get_required_env("GOOGLE_CLOUD_CLIENT_ID"),
        "auth_uri": _get_required_env("GOOGLE_CLOUD_AUTH_URI"),
        "token_uri": _get_required_env("GOOGLE_CLOUD_TOKEN_URI"),
        "auth_provider_x509_cert_url": _get_required_env("GOOGLE_CLOUD_AUTH_PROVIDER_X509_CERT_URL"),
        "client_x509_cert_url": _get_required_env("GOOGLE_CLOUD_CLIENT_X509_CERT_URL"),
    }

    runtime_dir = Path(".runtime")
    runtime_dir.mkdir(parents=True, exist_ok=True)
    credentials_path = runtime_dir / "gcp_service_account.json"
    credentials_path.write_text(json.dumps(credentials), encoding="utf-8")
    try:
        credentials_path.chmod(0o600)
    except OSError:
        # Best effort only; permission model differs across platforms.
        pass

    absolute_path = str(credentials_path.resolve())
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = absolute_path
    return absolute_path


def main() -> None:
    prepare_credentials_file()
    print("GOOGLE_APPLICATION_CREDENTIALS prepared")


if __name__ == "__main__":
    main()

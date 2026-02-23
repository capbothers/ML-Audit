"""Bootstrap credential files from environment variables.

On Render (and similar PaaS), credential JSON files can't be committed
to git.  Instead, paste the JSON content into env vars and this module
writes them to the expected file paths at startup.

The module checks two naming conventions so users don't need to create
extra env vars:

    Primary (explicit JSON vars):
        GA4_CREDENTIALS_JSON            → ./credentials/ga4-credentials.json
        GSC_CREDENTIALS_JSON            → ./credentials/gsc-credentials.json
        GOOGLE_SHEETS_CREDENTIALS_JSON  → ./credentials/google-sheets-credentials.json
        GOOGLE_SHEETS_SA_JSON           → ./credentials/google-sheets-sa.json

    Fallback (existing _PATH vars — if value looks like JSON, write it):
        GA4_CREDENTIALS_PATH            → ./credentials/ga4-credentials.json
        GSC_CREDENTIALS_PATH            → ./credentials/gsc-credentials.json
        GOOGLE_SHEETS_CREDENTIALS_PATH  → ./credentials/google-sheets-credentials.json
        MERCHANT_CENTER_CREDENTIALS_PATH → ./credentials/google-sheets-sa.json
"""
import json
import os

from app.utils.logger import log

# Maps env var name → target file path
# Each entry is (primary_env_var, fallback_env_var, file_path)
_CREDENTIAL_MAP = [
    ("GA4_CREDENTIALS_JSON", "GA4_CREDENTIALS_PATH", "./credentials/ga4-credentials.json"),
    ("GSC_CREDENTIALS_JSON", "GSC_CREDENTIALS_PATH", "./credentials/gsc-credentials.json"),
    ("GOOGLE_SHEETS_CREDENTIALS_JSON", "GOOGLE_SHEETS_CREDENTIALS_PATH", "./credentials/google-sheets-credentials.json"),
    ("GOOGLE_SHEETS_SA_JSON", "MERCHANT_CENTER_CREDENTIALS_PATH", "./credentials/google-sheets-sa.json"),
]


def _is_json(value: str) -> bool:
    """Check if a string looks like JSON content (not a file path)."""
    stripped = value.strip()
    return stripped.startswith("{") and stripped.endswith("}")


def bootstrap_credentials():
    """Write credential JSON files from env vars if the files don't exist.

    Strategy:
    1. For each target file, check primary then fallback env var for JSON content.
    2. If a GOOGLE_SA_JSON (single shared service account) var is set, use it as
       a catch-all for any missing credential files — most GCP setups share one SA.
    """
    os.makedirs("./credentials", exist_ok=True)

    # Shared service account fallback (covers GA4, GSC, Sheets, Merchant Center)
    shared_sa = os.environ.get("GOOGLE_SA_JSON", "")
    if not _is_json(shared_sa):
        shared_sa = ""

    for primary_var, fallback_var, file_path in _CREDENTIAL_MAP:
        if os.path.exists(file_path):
            log.info(f"Credential file {file_path} already exists, skipping")
            continue

        # Try primary env var, then fallback, then shared SA
        json_str = None
        source_var = None
        for var in (primary_var, fallback_var, "GOOGLE_SA_JSON"):
            value = os.environ.get(var, "")
            if value and _is_json(value):
                json_str = value
                source_var = var
                break

        if not json_str:
            continue

        try:
            json.loads(json_str)  # Validate it's real JSON
            with open(file_path, "w") as f:
                f.write(json_str)
            log.info(f"Wrote {file_path} from {source_var}")
        except json.JSONDecodeError:
            log.error(f"{source_var} is not valid JSON, skipping")
        except Exception as e:
            log.error(f"Failed to write {file_path} from {source_var}: {e}")

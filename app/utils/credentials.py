"""Bootstrap credential files from environment variables.

On Render (and similar PaaS), credential JSON files can't be committed
to git.  Instead, paste the JSON content into env vars and this module
writes them to the expected file paths at startup.

Env var → file mapping:
    GA4_CREDENTIALS_JSON            → ./credentials/ga4-credentials.json
    GSC_CREDENTIALS_JSON            → ./credentials/gsc-credentials.json
    GOOGLE_SHEETS_CREDENTIALS_JSON  → ./credentials/google-sheets-credentials.json
    GOOGLE_SHEETS_SA_JSON           → ./credentials/google-sheets-sa.json
"""
import json
import os

from app.utils.logger import log

# Maps env var name → target file path
_CREDENTIAL_MAP = {
    "GA4_CREDENTIALS_JSON": "./credentials/ga4-credentials.json",
    "GSC_CREDENTIALS_JSON": "./credentials/gsc-credentials.json",
    "GOOGLE_SHEETS_CREDENTIALS_JSON": "./credentials/google-sheets-credentials.json",
    "GOOGLE_SHEETS_SA_JSON": "./credentials/google-sheets-sa.json",
}


def bootstrap_credentials():
    """Write credential JSON files from env vars if the files don't exist."""
    os.makedirs("./credentials", exist_ok=True)

    for env_var, file_path in _CREDENTIAL_MAP.items():
        json_str = os.environ.get(env_var)
        if not json_str:
            continue

        if os.path.exists(file_path):
            log.info(f"Credential file {file_path} already exists, skipping {env_var}")
            continue

        try:
            # Validate it's real JSON before writing
            json.loads(json_str)
            with open(file_path, "w") as f:
                f.write(json_str)
            log.info(f"Wrote {file_path} from {env_var}")
        except json.JSONDecodeError:
            log.error(f"{env_var} is not valid JSON, skipping")
        except Exception as e:
            log.error(f"Failed to write {file_path} from {env_var}: {e}")

import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()

@dataclass
class Config:
    # Google Drive
    google_credentials_path: str = field(default_factory=lambda: os.environ["GOOGLE_CREDENTIALS_PATH"])
    google_token_path: str = field(default_factory=lambda: os.getenv("GOOGLE_TOKEN_PATH", "ops/credentials/token.json"))
    shared_drive_id: str = field(default_factory=lambda: os.getenv("SHARED_DRIVE_ID", "personal"))
    target_folder_id: str = field(default_factory=lambda: os.getenv("TARGET_FOLDER_ID", ""))

    # PostgreSQL
    postgres_dsn: str = field(default_factory=lambda: os.environ["POSTGRES_DSN"])

    # AutoDraft_clean API
    clean_api_url: str = field(default_factory=lambda: os.environ["CLEAN_API_URL"])

    # Ingest
    max_retry: int = field(default_factory=lambda: int(os.getenv("MAX_RETRY", "3")))
    poll_interval_seconds: int = field(default_factory=lambda: int(os.getenv("POLL_INTERVAL_SECONDS", "300")))

config = Config()

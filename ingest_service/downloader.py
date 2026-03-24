"""Downloads Drive files and computes content hash."""

import hashlib
from ingest_service.drive_client import DriveClient


class Downloader:
    def __init__(self, drive_client: DriveClient):
        self._client = drive_client

    def download(self, file_id: str) -> tuple[bytes, str]:
        """Returns (raw_bytes, sha256_hex)."""
        data = self._client.download(file_id)
        content_hash = hashlib.sha256(data).hexdigest()
        return data, content_hash

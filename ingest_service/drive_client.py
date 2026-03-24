"""Google Drive API client using OAuth 2.0 (Desktop app / personal Drive)."""

import io
import os
from datetime import datetime
from typing import Generator

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

from shared.config import config
from shared.models import DriveFile

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

_PERSONAL_DRIVE_ID = "personal"


def _build_service():
    creds = None
    token_path = config.google_token_path

    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                config.google_credentials_path, SCOPES
            )
            creds = flow.run_local_server(port=0)
        os.makedirs(os.path.dirname(token_path), exist_ok=True)
        with open(token_path, "w") as f:
            f.write(creds.to_json())

    return build("drive", "v3", credentials=creds)


class DriveClient:
    def __init__(self):
        self._svc = _build_service()

    def list_pdf_files(self, folder_id: str = "") -> Generator[DriveFile, None, None]:
        """List all PDF files in the target folder (or entire personal Drive)."""
        query = "mimeType='application/pdf' and trashed=false"
        if folder_id:
            query += f" and '{folder_id}' in parents"

        page_token = None
        while True:
            resp = self._svc.files().list(
                q=query,
                fields="nextPageToken,files(id,name,mimeType,modifiedTime,headRevisionId,parents)",
                pageToken=page_token,
            ).execute()

            for f in resp.get("files", []):
                yield DriveFile(
                    file_id=f["id"],
                    file_name=f["name"],
                    drive_id=_PERSONAL_DRIVE_ID,
                    mime_type=f["mimeType"],
                    modified_time=datetime.fromisoformat(f["modifiedTime"].replace("Z", "+00:00")),
                    revision_id=f.get("headRevisionId"),
                )

            page_token = resp.get("nextPageToken")
            if not page_token:
                break

    def list_changes(self, page_token: str) -> tuple[list[dict], str]:
        """Return (changes, new_page_token). Each change: {fileId, file, removed}."""
        changes = []
        while True:
            resp = self._svc.changes().list(
                pageToken=page_token,
                fields="nextPageToken,newStartPageToken,changes(fileId,removed,file(id,name,mimeType,modifiedTime,headRevisionId,trashed,parents))",
            ).execute()

            changes.extend(resp.get("changes", []))
            page_token = resp.get("nextPageToken")
            if not page_token:
                return changes, resp.get("newStartPageToken", "")

    def get_start_page_token(self) -> str:
        resp = self._svc.changes().getStartPageToken().execute()
        return resp["startPageToken"]

    def get_file(self, file_id: str) -> "DriveFile":
        """단일 파일 메타데이터 조회."""
        f = self._svc.files().get(
            fileId=file_id,
            fields="id,name,mimeType,modifiedTime,headRevisionId,parents",
        ).execute()
        return DriveFile(
            file_id=f["id"],
            file_name=f["name"],
            drive_id=_PERSONAL_DRIVE_ID,
            mime_type=f["mimeType"],
            modified_time=datetime.fromisoformat(f["modifiedTime"].replace("Z", "+00:00")),
            revision_id=f.get("headRevisionId"),
        )

    def download(self, file_id: str) -> bytes:
        request = self._svc.files().get_media(fileId=file_id)
        buf = io.BytesIO()
        downloader = MediaIoBaseDownload(buf, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        return buf.getvalue()

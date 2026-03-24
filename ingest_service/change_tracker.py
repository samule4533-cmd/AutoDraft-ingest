"""Manages Drive page tokens and detects new/modified/deleted files."""

from datetime import datetime
from typing import Optional
import psycopg2
from psycopg2.extras import RealDictCursor

from shared.config import config
from shared.models import DriveFile
from ingest_service.drive_client import DriveClient


class ChangeTracker:
    def __init__(self, drive_client: DriveClient):
        self._client = drive_client
        self._conn = psycopg2.connect(config.postgres_dsn)

    def _get_stored_token(self, drive_id: str) -> Optional[str]:
        with self._conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT page_token FROM drive_sync_state WHERE drive_id = %s",
                (drive_id,),
            )
            row = cur.fetchone()
            return row["page_token"] if row else None

    def _save_token(self, drive_id: str, token: str) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO drive_sync_state (drive_id, page_token, last_synced_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (drive_id) DO UPDATE
                SET page_token = EXCLUDED.page_token,
                    last_synced_at = NOW()
                """,
                (drive_id, token),
            )
        self._conn.commit()

    def initialize_if_needed(self) -> None:
        """Bootstrap page token on first run."""
        drive_id = config.shared_drive_id
        token = self._get_stored_token(drive_id)
        if token is None:
            token = self._client.get_start_page_token()
            self._save_token(drive_id, token)

    def get_changes(self) -> tuple[list[DriveFile], list[str], Optional[str]]:
        """
        Returns:
            upsert_files: new or modified PDF files to process
            deleted_file_ids: file IDs that were deleted/trashed
            new_token: 처리 완료 후 저장할 토큰 (None이면 저장 불필요)

        첫 실행(token 없음): 대상 폴더 전체 스캔 후 start token 바로 저장.
        이후 실행: page token 기반 변경분만 감지. 토큰은 반환만 하고 저장은 caller가 담당.
        """
        drive_id = config.shared_drive_id
        token = self._get_stored_token(drive_id)

        if token is None:
            # 첫 실행 — 기존 파일 전체 스캔 후 현재 시점 토큰 저장
            existing = list(self._client.list_pdf_files(folder_id=config.target_folder_id))
            start_token = self._client.get_start_page_token()
            self._save_token(drive_id, start_token)
            return existing, [], None

        raw_changes, new_token = self._client.list_changes(token)
        # 토큰은 여기서 저장하지 않는다.
        # 처리 완료 후 worker가 commit_token()을 호출해야 저장된다.
        # 처리 도중 서버가 꺼지면 다음 실행에서 같은 변경사항을 다시 감지한다.

        upsert_files: list[DriveFile] = []
        deleted_ids: list[str] = []

        target_folder = config.target_folder_id

        for change in raw_changes:
            file_meta = change.get("file", {})

            # 대상 폴더가 지정된 경우 해당 폴더의 파일만 처리
            if target_folder:
                parents = file_meta.get("parents", [])
                if not change.get("removed") and target_folder not in parents:
                    continue

            if change.get("removed") or file_meta.get("trashed"):
                deleted_ids.append(change["fileId"])
            elif file_meta.get("mimeType") == "application/pdf":
                upsert_files.append(DriveFile(
                    file_id=file_meta["id"],
                    file_name=file_meta["name"],
                    drive_id=drive_id,
                    mime_type=file_meta["mimeType"],
                    modified_time=datetime.fromisoformat(
                        file_meta["modifiedTime"].replace("Z", "+00:00")
                    ),
                    revision_id=file_meta.get("headRevisionId"),
                ))

        return upsert_files, deleted_ids, new_token

    def commit_token(self, new_token: str) -> None:
        """모든 변경사항 처리 완료 후 page token을 저장한다."""
        self._save_token(config.shared_drive_id, new_token)

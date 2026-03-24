"""PostgreSQL-backed document state store."""

from datetime import datetime
from typing import Optional
import psycopg2
from psycopg2.extras import RealDictCursor

from shared.config import config
from shared.models import DocStatus, DocumentState


class StateStore:
    def __init__(self):
        self._conn = psycopg2.connect(config.postgres_dsn)

    # ── read ──────────────────────────────────────────────────────────────

    def get(self, file_id: str) -> Optional[DocumentState]:
        with self._conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM document_state WHERE file_id = %s", (file_id,)
            )
            row = cur.fetchone()
        return _row_to_state(row) if row else None

    def needs_processing(self, file_id: str, revision_id: str) -> bool:
        """True if file is new OR revision changed OR previously failed under retry limit."""
        state = self.get(file_id)
        if state is None:
            return True
        if state.status == DocStatus.DELETED:
            return True
        if state.revision_id != revision_id:
            return True
        if state.status == DocStatus.FAILED and state.retry_count < config.max_retry:
            return True
        return False

    def list_by_status(self, status: DocStatus) -> list[DocumentState]:
        with self._conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM document_state WHERE status = %s ORDER BY updated_at DESC",
                (status.value,),
            )
            return [_row_to_state(r) for r in cur.fetchall()]

    # ── write ─────────────────────────────────────────────────────────────

    def upsert_pending(self, file_id: str, file_name: str, drive_id: str,
                       mime_type: str, modified_time: datetime,
                       revision_id: Optional[str], folder_path: Optional[str] = None) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO document_state
                    (file_id, file_name, drive_id, mime_type, modified_time,
                     revision_id, folder_path, status, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'PENDING', NOW())
                ON CONFLICT (file_id) DO UPDATE SET
                    file_name     = EXCLUDED.file_name,
                    mime_type     = EXCLUDED.mime_type,
                    modified_time = EXCLUDED.modified_time,
                    revision_id   = EXCLUDED.revision_id,
                    folder_path   = EXCLUDED.folder_path,
                    status        = 'PENDING',
                    version       = document_state.version + 1,
                    updated_at    = NOW()
                WHERE document_state.revision_id IS DISTINCT FROM EXCLUDED.revision_id
                   OR document_state.status = 'FAILED'
                """,
                (file_id, file_name, drive_id, mime_type,
                 modified_time, revision_id, folder_path),
            )
        self._conn.commit()

    def reset_stuck_processing(self) -> int:
        """PROCESSING 상태로 멈춘 파일을 PENDING으로 리셋한다.
        서버 재시작 시 호출하여 이전 실행에서 중단된 파일을 재처리 대상으로 만든다."""
        with self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE document_state
                SET status = 'PENDING', updated_at = NOW()
                WHERE status = 'PROCESSING'
                """,
            )
            count = cur.rowcount
        self._conn.commit()
        return count

    def set_processing(self, file_id: str) -> None:
        self._set_status(file_id, DocStatus.PROCESSING)

    def set_ready(self, file_id: str, chunk_count: int, content_hash: str) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE document_state
                SET status = 'READY', chunk_count = %s, content_hash = %s,
                    error_message = NULL, processed_at = NOW(), updated_at = NOW()
                WHERE file_id = %s
                """,
                (chunk_count, content_hash, file_id),
            )
        self._conn.commit()

    def set_failed(self, file_id: str, error: str) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE document_state
                SET status = 'FAILED', error_message = %s,
                    retry_count = retry_count + 1, updated_at = NOW()
                WHERE file_id = %s
                """,
                (error[:2000], file_id),
            )
        self._conn.commit()

    def update_file_name(self, file_id: str, file_name: str) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                "UPDATE document_state SET file_name = %s, updated_at = NOW() WHERE file_id = %s",
                (file_name, file_id),
            )
        self._conn.commit()

    def set_deleted(self, file_id: str) -> None:
        self._set_status(file_id, DocStatus.DELETED)

    def log_event(self, file_id: str, event: str, detail: str = "") -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                "INSERT INTO ingest_log (file_id, event, detail) VALUES (%s, %s, %s)",
                (file_id, event, detail),
            )
        self._conn.commit()

    def _set_status(self, file_id: str, status: DocStatus) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                "UPDATE document_state SET status = %s, updated_at = NOW() WHERE file_id = %s",
                (status.value, file_id),
            )
        self._conn.commit()


def _row_to_state(row: dict) -> DocumentState:
    return DocumentState(
        file_id=row["file_id"],
        file_name=row["file_name"],
        drive_id=row["drive_id"],
        status=DocStatus(row["status"]),
        version=row["version"],
        modified_time=row.get("modified_time"),
        revision_id=row.get("revision_id"),
        content_hash=row.get("content_hash"),
        mime_type=row.get("mime_type"),
        chunk_count=row.get("chunk_count"),
        error_message=row.get("error_message"),
        retry_count=row["retry_count"],
        folder_path=row.get("folder_path"),
        created_at=row.get("created_at"),
        updated_at=row.get("updated_at"),
        processed_at=row.get("processed_at"),
    )

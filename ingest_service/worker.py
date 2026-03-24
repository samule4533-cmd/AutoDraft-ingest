"""
Main ingest pipeline worker.
Orchestrates: Drive changes -> download -> call AutoDraft_clean API -> state update
"""

import logging
import time

import httpx

from shared.config import config
from shared.models import DocStatus
from ingest_service.drive_client import DriveClient
from ingest_service.change_tracker import ChangeTracker
from ingest_service.downloader import Downloader
from ingest_service.state_store import StateStore

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


class IngestWorker:
    def __init__(self):
        drive_client = DriveClient()
        self._tracker = ChangeTracker(drive_client)
        self._downloader = Downloader(drive_client)
        self._state = StateStore()

    def run_once(self) -> None:
        log.info("Polling Drive for changes...")
        upsert_files, deleted_ids = self._tracker.get_changes()
        log.info(f"  -> {len(upsert_files)} upserts, {len(deleted_ids)} deletes")

        for file_id in deleted_ids:
            self._handle_delete(file_id)

        for drive_file in upsert_files:
            if not self._state.needs_processing(drive_file.file_id, drive_file.revision_id or ""):
                # revision 동일 → 내용 변경 없음. 파일명만 바뀌었는지 확인.
                state = self._state.get(drive_file.file_id)
                if state and state.file_name != drive_file.file_name:
                    log.info(f"파일명 변경 감지: {state.file_name} → {drive_file.file_name}")
                    self._state.update_file_name(drive_file.file_id, drive_file.file_name)
                    self._state.log_event(drive_file.file_id, "NAME_CHANGED", f"{state.file_name} → {drive_file.file_name}")
                else:
                    log.info(f"Skipping unchanged file: {drive_file.file_name}")
                continue
            self._handle_upsert(drive_file)

        # FAILED 상태 파일 재시도
        failed = self._state.list_by_status(DocStatus.FAILED)
        retryable = [s for s in failed if s.retry_count < config.max_retry]
        if retryable:
            log.info(f"FAILED 파일 재시도: {len(retryable)}개")
        for state in retryable:
            try:
                drive_file = self._tracker._client.get_file(state.file_id)
                self._handle_upsert(drive_file)
            except Exception as e:
                log.error(f"재시도 실패: {state.file_name}: {e}")

    def run_loop(self) -> None:
        self._tracker.initialize_if_needed()
        while True:
            try:
                self.run_once()
            except Exception as e:
                log.error(f"Poll cycle failed: {e}", exc_info=True)
            time.sleep(config.poll_interval_seconds)

    # ── private ────────────────────────────────────────────────────────────

    def _handle_upsert(self, drive_file) -> None:
        fid = drive_file.file_id
        fname = drive_file.file_name
        log.info(f"Processing: {fname} ({fid})")

        self._state.upsert_pending(
            file_id=fid,
            file_name=fname,
            drive_id=drive_file.drive_id,
            mime_type=drive_file.mime_type,
            modified_time=drive_file.modified_time,
            revision_id=drive_file.revision_id,
            folder_path=drive_file.folder_path,
        )

        state = self._state.get(fid)
        version = state.version if state else 1

        try:
            self._state.set_processing(fid)
            self._state.log_event(fid, "STARTED")

            # 1. Download
            raw_bytes, content_hash = self._downloader.download(fid)
            self._state.log_event(fid, "DOWNLOADED", f"size={len(raw_bytes)}")

            # 2. AutoDraft_clean API로 파싱/청킹/임베딩/적재 위임
            chunk_count = self._call_clean_ingest(fid, fname, raw_bytes)
            self._state.log_event(fid, "INGESTED", f"chunks={chunk_count}")

            # 3. Mark READY
            self._state.set_ready(fid, chunk_count, content_hash)
            self._state.log_event(fid, "COMPLETED", f"version={version}")
            log.info(f"Done: {fname} -> {chunk_count} chunks, version {version}")

        except Exception as e:
            log.error(f"Failed: {fname}: {e}", exc_info=True)
            self._state.set_failed(fid, str(e))
            self._state.log_event(fid, "FAILED", str(e))

    def _call_clean_ingest(self, file_id: str, file_name: str, raw_bytes: bytes) -> int:
        """AutoDraft_clean의 ingest 엔드포인트에 파일을 전송하고 청크 수를 반환한다."""
        url = f"{config.clean_api_url}/ingest"
        with httpx.Client(timeout=120) as client:
            response = client.post(
                url,
                files={"file": (file_name, raw_bytes, "application/pdf")},
                data={"file_id": file_id, "file_name": file_name},
            )
            response.raise_for_status()
            return response.json().get("chunk_count", 0)

    def _handle_delete(self, file_id: str) -> None:
        log.info(f"Deleting file from index: {file_id}")
        url = f"{config.clean_api_url}/ingest/{file_id}"
        try:
            with httpx.Client(timeout=30) as client:
                client.delete(url).raise_for_status()
        except Exception as e:
            log.error(f"Clean API 삭제 실패: {file_id}: {e}")
        self._state.set_deleted(file_id)
        self._state.log_event(file_id, "DELETED")


if __name__ == "__main__":
    worker = IngestWorker()
    worker.run_loop()

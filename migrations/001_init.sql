CREATE TYPE doc_status AS ENUM ('PENDING', 'PROCESSING', 'READY', 'FAILED', 'DELETED');

CREATE TABLE IF NOT EXISTS document_state (
    file_id          TEXT PRIMARY KEY,
    file_name        TEXT NOT NULL,
    drive_id         TEXT NOT NULL,
    folder_path      TEXT,
    status           doc_status NOT NULL DEFAULT 'PENDING',
    version          INTEGER NOT NULL DEFAULT 1,
    modified_time    TIMESTAMPTZ,
    revision_id      TEXT,
    content_hash     TEXT,
    mime_type        TEXT,
    chunk_count      INTEGER,
    error_message    TEXT,
    retry_count      INTEGER NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_doc_state_status ON document_state(status);
CREATE INDEX IF NOT EXISTS idx_doc_state_drive ON document_state(drive_id);
CREATE INDEX IF NOT EXISTS idx_doc_state_updated ON document_state(updated_at);

CREATE TABLE IF NOT EXISTS drive_sync_state (
    drive_id         TEXT PRIMARY KEY,
    page_token       TEXT NOT NULL,
    last_synced_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ingest_log (
    id               BIGSERIAL PRIMARY KEY,
    file_id          TEXT NOT NULL REFERENCES document_state(file_id) ON DELETE CASCADE,
    event            TEXT NOT NULL,  -- 'STARTED', 'PARSED', 'CHUNKED', 'EMBEDDED', 'COMPLETED', 'FAILED'
    detail           TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ingest_log_file ON ingest_log(file_id);

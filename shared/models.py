from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional

class DocStatus(str, Enum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    READY = "READY"
    FAILED = "FAILED"
    DELETED = "DELETED"

@dataclass
class DriveFile:
    file_id: str
    file_name: str
    drive_id: str
    mime_type: str
    modified_time: datetime
    revision_id: Optional[str] = None
    folder_path: Optional[str] = None

@dataclass
class DocumentState:
    file_id: str
    file_name: str
    drive_id: str
    status: DocStatus
    version: int = 1
    modified_time: Optional[datetime] = None
    revision_id: Optional[str] = None
    content_hash: Optional[str] = None
    mime_type: Optional[str] = None
    chunk_count: Optional[int] = None
    error_message: Optional[str] = None
    retry_count: int = 0
    folder_path: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    processed_at: Optional[datetime] = None


import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from ingest_service.worker import IngestWorker

worker = IngestWorker()
worker.run_once()

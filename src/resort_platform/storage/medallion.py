from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from google.cloud import storage

from resort_platform.config import get_settings


@dataclass(frozen=True)
class LandingObject:
    bucket: str
    object_name: str
    generation: int | None
    md5: str


class MedallionStorage:
    def __init__(self, client: storage.Client | None = None):
        self.settings = get_settings()
        self.client = client or storage.Client(project=self.settings.gcp_project_id)

    def land_json(self, *, domain: str, entity: str, business_key: str, payload: dict[str, Any]) -> LandingObject:
        now = datetime.now(timezone.utc)
        encoded = json.dumps(payload, default=str, sort_keys=True, separators=(",", ":")).encode()
        digest = hashlib.sha256(encoded).hexdigest()
        event_id = str(payload.get("event_id", digest[:24]))
        object_name = (
            f"landing/{domain}/{entity}/event_date={now.date().isoformat()}/"
            f"business_key={business_key}/{event_id}.json"
        )
        bucket = self.client.bucket(self.settings.gcs_landing_bucket)
        blob = bucket.blob(object_name)
        blob.metadata = {
            "sha256": digest,
            "schema_version": str(payload.get("schema_version", "1")),
            "trace_id": str(payload.get("trace_id", "")),
        }
        blob.upload_from_string(encoded, content_type="application/json", if_generation_match=0)
        return LandingObject(bucket=bucket.name, object_name=object_name, generation=blob.generation, md5=digest)

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal

from google.cloud import storage

from resort_platform.config import get_settings

Layer = Literal["landing", "bronze", "silver", "gold", "quarantine"]


@dataclass(frozen=True)
class StoredObject:
    layer: Layer
    bucket: str
    object_name: str
    generation: int | None
    sha256: str


class MedallionStorage:
    def __init__(self, client: storage.Client | None = None):
        self.settings = get_settings()
        self.client = client or storage.Client(project=self.settings.gcp_project_id)

    def _bucket_name(self, layer: Layer) -> str:
        return {
            "landing": self.settings.gcs_landing_bucket,
            "bronze": self.settings.gcs_bronze_bucket,
            "silver": self.settings.gcs_silver_bucket,
            "gold": self.settings.gcs_gold_bucket,
            "quarantine": self.settings.gcs_quarantine_bucket,
        }[layer]

    def write_json(
        self,
        *,
        layer: Layer,
        domain: str,
        entity: str,
        business_key: str,
        payload: dict[str, Any],
        immutable: bool = True,
    ) -> StoredObject:
        now = datetime.now(timezone.utc)
        encoded = json.dumps(payload, default=str, sort_keys=True, separators=(",", ":")).encode()
        digest = hashlib.sha256(encoded).hexdigest()
        event_id = str(payload.get("event_id", digest[:24]))
        object_name = (
            f"{layer}/{domain}/{entity}/event_date={now.date().isoformat()}/"
            f"hour={now.hour:02d}/business_key={business_key}/{event_id}.json"
        )
        bucket = self.client.bucket(self._bucket_name(layer))
        blob = bucket.blob(object_name)
        blob.metadata = {
            "sha256": digest,
            "schema_version": str(payload.get("schema_version", "1")),
            "trace_id": str(payload.get("trace_id", "")),
            "event_id": event_id,
        }
        kwargs = {"if_generation_match": 0} if immutable else {}
        blob.upload_from_string(encoded, content_type="application/json", **kwargs)
        return StoredObject(layer=layer, bucket=bucket.name, object_name=object_name, generation=blob.generation, sha256=digest)

    def land_json(self, *, domain: str, entity: str, business_key: str, payload: dict[str, Any]) -> StoredObject:
        return self.write_json(layer="landing", domain=domain, entity=entity, business_key=business_key, payload=payload)

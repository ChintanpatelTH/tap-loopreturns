"""Stream type classes for tap-loopreturns."""

from __future__ import annotations

from pathlib import Path

from tap_loopreturns.client import LoopReturnsStream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

class ReturnsStream(LoopReturnsStream):
    """Define custom stream."""

    name = "returns"
    path = "/warehouse/return/list"
    primary_keys = ["id"]  # noqa: RUF012
    replication_key = "updated_at"
    records_jsonpath = "$.returns[*]"
    replication_method = "INCREMENTAL"
    schema_filepath = SCHEMAS_DIR / "returns.json"
    is_sorted = False

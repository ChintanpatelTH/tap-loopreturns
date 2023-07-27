"""LoopReturns tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_loopreturns import streams


class TapLoopReturns(Tap):
    """LoopReturns tap class."""

    name = "tap-loopreturns"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            description="The api key to authenticate against the API service",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
        ),
        th.Property(
            "backfill_interval",
            th.IntegerType,
            description="The number of days to backfill since last sync date",
        ),
        th.Property(
            "api_url",
            th.StringType,
            default="https://api.loopreturns.com/api/v1",
            description="The url for the API service",
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.LoopReturnsStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.ReturnsStream(self),
        ]


if __name__ == "__main__":
    TapLoopReturns.cli()

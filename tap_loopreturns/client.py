"""REST client handling, including LoopReturnsStream base class."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable
from urllib.parse import parse_qsl, urlsplit

import requests
from dateutil import parser
from singer_sdk.authenticators import APIKeyAuthenticator
from singer_sdk.streams import RESTStream

_Auth = Callable[[requests.PreparedRequest], requests.PreparedRequest]
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
PAGE_SIZE = 100


class LoopReturnsStream(RESTStream):
    """LoopReturns stream class."""

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config.get("api_url")

    records_jsonpath = "$[*]"  # Or override `parse_response`.

    # Set this value or override `get_new_paginator`.
    next_page_token_jsonpath = "$.nextPageUrl"  # noqa: S105
    start_date: str | None = None
    end_date: str | None = None

    @property
    def authenticator(self) -> APIKeyAuthenticator:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return APIKeyAuthenticator.create_for_stream(
            self,
            key="X-Authorization",
            value=self.config.get("api_key", ""),
            location="header",
        )

    def get_url_params(
        self,
        context: dict | None,  # noqa: ARG002
        next_page_token: Any | None,  # noqa: ANN401
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {}
        if next_page_token:
            return dict(parse_qsl(urlsplit(next_page_token).query))

        params["from"] = self.start_date
        params["to"] = self.end_date
        params["paginate"] = True
        params["pageSize"] = PAGE_SIZE
        params["filter"] = "updated_at"
        return params

    def get_records(self, context: dict) -> Iterable[dict[str, Any]]:
        """Return a generator of row-type dictionary objects.

        Args:
            context: The stream context.

        Yields:
            Each record from the source.
        """
        current_state = self.get_context_state(context)
        current_date = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
        interval = float(self.config.get("backfill_interval", 1))
        min_value = current_state.get(
            "replication_key_value",
            self.config.get("start_date", ""),
        )
        context = context or {}
        # set from date to last updated date or config start date
        min_date = parser.parse(min_value).replace(tzinfo=None)
        while min_date < current_date:
            updated_at_max = min_date + timedelta(days=interval)
            if updated_at_max > current_date:
                updated_at_max = current_date

            self.start_date = min_date.isoformat()
            self.end_date = updated_at_max.isoformat()
            yield from super().get_records(context)
            # Send state message
            self._increment_stream_state({"updated_at": self.end_date}, context=context)
            self._write_state_message()
            min_date = updated_at_max

"""REST client handling, including LoopReturnsStream base class."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qsl, urlsplit

import requests
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
        context: dict | None,
        next_page_token: Any | None,
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
        if self.replication_key:
            current_date = datetime.now(timezone.utc).replace(microsecond=0)
            context_state = self.get_context_state(context)
            last_updated = context_state.get("replication_key_value")
            interval = self.config.get("backfill_interval")
            config_start_date = self.config.get("start_date")
            # set from date to last updated date or config start date
            start_date = datetime.strptime(
                (last_updated if last_updated else config_start_date),
                "%Y-%m-%dT%H:%M:%S%z",
            ) + timedelta(seconds=1) # Add 1 second to last updated date to avoid duplicates  # noqa: E501
            params["from"] = start_date.strftime("%Y-%m-%dT%H:%M:%S")
            # set to date to current date or start date + interval
            if interval:
                params["to"] = (
                    current_date.strftime("%Y-%m-%dT%H:%M:%S")
                    if start_date + timedelta(days=interval) > current_date
                    else (start_date + timedelta(days=interval)).strftime(
                        "%Y-%m-%dT%H:%M:%S",
                    )
                )
            params["paginate"] = True
            params["pageSize"] = PAGE_SIZE
            params["filter"] = "updated_at"
        return params

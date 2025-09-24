# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.


import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception
from typing import Iterator, Dict, Any, Optional, cast
from datetime import datetime

from load_clinicaltrialsgov.config import settings

BASE_URL = "https://clinicaltrials.gov/api/v2/studies"


def _is_retryable_exception(exception: BaseException) -> bool:
    """
    Determines if an exception is retryable.

    Returns True for network timeouts, connection errors, 429 (rate limiting),
    and 5xx server errors.
    """
    if isinstance(exception, (httpx.TimeoutException, httpx.ConnectError)):
        return True
    if isinstance(exception, httpx.HTTPStatusError):
        status_code = exception.response.status_code
        return status_code == 429 or 500 <= status_code < 600
    return False


class APIClient:
    """
    A client for interacting with the ClinicalTrials.gov V2 API.
    """

    def __init__(self) -> None:
        self.client = httpx.Client(
            timeout=settings.api.timeout,
            limits=httpx.Limits(max_connections=5, max_keepalive_connections=5),
        )

    @retry(
        stop=stop_after_attempt(settings.api.max_retries),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception(_is_retryable_exception),
    )
    def _fetch_page(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Fetches a single page of studies from the API as a raw dictionary.
        """
        response = self.client.get(BASE_URL, params=params)
        response.raise_for_status()
        return cast(Dict[str, Any], response.json())

    def get_all_studies(
        self, updated_since: Optional[datetime] = None
    ) -> Iterator[Dict[str, Any]]:
        """
        Fetches all studies from the API, yielding raw study dictionaries.

        Args:
            updated_since: If provided, only fetch studies updated since this timestamp.
        """
        params = {}
        if updated_since:
            # Format date as YYYY-MM-DD
            date_str = updated_since.strftime("%Y-%m-%d")
            params["filter.advanced"] = f"AREA[LastUpdatePostDate]RANGE[{date_str},MAX]"

        page_token = None
        while True:
            if page_token:
                params["pageToken"] = page_token

            api_response = self._fetch_page(params)

            for study_dict in api_response.get("studies", []):
                yield study_dict

            page_token = api_response.get("nextPageToken")
            if not page_token:
                break

    def close(self) -> None:
        self.client.close()

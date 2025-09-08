import httpx
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)
from typing import Iterator, Dict, Any, Optional
from datetime import datetime

from py_load_clinicaltrialsgov.config import settings
from py_load_clinicaltrialsgov.models.api_models import APIResponse, Study

BASE_URL = "https://clinicaltrials.gov/api/v2/studies"


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
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.HTTPStatusError)),
    )
    def _fetch_page(self, params: Dict[str, Any]) -> APIResponse:
        """
        Fetches a single page of studies from the API.
        """
        response = self.client.get(BASE_URL, params=params)
        response.raise_for_status()
        return APIResponse.model_validate(response.json())

    def get_all_studies(
        self, updated_since: Optional[datetime] = None
    ) -> Iterator[Study]:
        """
        Fetches all studies from the API, handling pagination.

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

            for study in api_response.studies:
                yield study

            page_token = api_response.next_page_token
            if not page_token:
                break

    def close(self) -> None:
        self.client.close()

# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.


import httpx
import pytest
from unittest.mock import MagicMock, patch

from load_clinicaltrialsgov.extractor.api_client import APIClient


def test_fetch_page_retries_on_retryable_exception() -> None:
    """
    Verify that _fetch_page retries on retryable exceptions.
    """
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = [
        httpx.TimeoutException("timeout"),
        httpx.TimeoutException("timeout"),
        mock_response,  # Success on the third attempt
    ]
    mock_response.json.return_value = {"studies": []}

    with patch("httpx.Client.get", return_value=mock_response) as mock_get:
        client = APIClient()
        client._fetch_page({})
        assert mock_get.call_count == 3


def test_fetch_page_does_not_retry_on_non_retryable_exception() -> None:
    """
    Verify that _fetch_page does not retry on non-retryable exceptions.
    """
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
        "Bad Request", request=MagicMock(), response=MagicMock(status_code=400)
    )

    with patch("httpx.Client.get", return_value=mock_response) as mock_get:
        client = APIClient()
        with pytest.raises(httpx.HTTPStatusError):
            client._fetch_page({})
        assert mock_get.call_count == 1

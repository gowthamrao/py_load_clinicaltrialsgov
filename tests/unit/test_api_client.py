import httpx
import pytest
from datetime import datetime
from typing import List, Tuple, Any


from load_clinicaltrialsgov.extractor.api_client import APIClient
from load_clinicaltrialsgov.config import settings

from httpx import MockTransport, Response


@pytest.fixture
def mock_transport() -> MockTransport:
    def handler(request: httpx.Request) -> Response:
        if "pageToken=next" in str(request.url):
            return Response(
                200,
                json={
                    "studies": [
                        {
                            "protocolSection": {
                                "identificationModule": {"nctId": "NCT00000002"}
                            },
                            "derivedSection": {},
                            "hasResults": False,
                        }
                    ],
                    "nextPageToken": None,
                },
            )
        if "filter.advanced" in str(request.url):
            return Response(
                200,
                json={
                    "studies": [
                        {
                            "protocolSection": {
                                "identificationModule": {"nctId": "NCT00000003"}
                            },
                            "derivedSection": {},
                            "hasResults": False,
                        }
                    ],
                    "nextPageToken": None,
                },
            )
        return Response(
            200,
            json={
                "studies": [
                    {
                        "protocolSection": {
                            "identificationModule": {"nctId": "NCT00000001"}
                        },
                        "derivedSection": {},
                        "hasResults": False,
                    }
                ],
                "nextPageToken": "next",
            },
        )

    return MockTransport(handler)


class MockStatefulTransport(MockTransport):
    def __init__(self, responses: List[Tuple[int, dict[str, Any]] | Exception]):
        self.responses = responses
        self.call_count = 0
        super().__init__(self.handler)

    def handler(self, request: httpx.Request) -> Response:
        response = self.responses[self.call_count]
        self.call_count += 1
        if isinstance(response, Exception):
            raise response
        status_code, json_data = response
        return Response(status_code=status_code, json=json_data)


def test_get_all_studies_pagination(mock_transport: MockTransport) -> None:
    client = APIClient()
    client.client = httpx.Client(transport=mock_transport)
    studies = list(client.get_all_studies())
    assert len(studies) == 2


def test_get_all_studies_delta_load(mock_transport: MockTransport) -> None:
    client = APIClient()
    client.client = httpx.Client(transport=mock_transport)
    studies = list(client.get_all_studies(updated_since=datetime(2023, 1, 1)))
    assert len(studies) == 1
    assert "protocolSection" in studies[0]
    assert (
        studies[0]["protocolSection"]["identificationModule"]["nctId"] == "NCT00000003"
    )


@pytest.mark.parametrize(
    "retryable_status_code",
    [
        429,  # Too Many Requests
        500,  # Internal Server Error
        503,  # Service Unavailable
    ],
)
def test_fetch_page_retries_on_retryable_errors(retryable_status_code: int) -> None:
    # Arrange
    responses: List[Tuple[int, dict[str, Any]] | Exception] = [
        (retryable_status_code, {"error": "transient error"}),
        (200, {"studies": [], "nextPageToken": None}),
    ]
    transport = MockStatefulTransport(responses)
    client = APIClient()
    client.client = httpx.Client(transport=transport)

    # Act
    client._fetch_page(params={})

    # Assert
    assert transport.call_count == 2


def test_fetch_page_retries_on_timeout() -> None:
    # Arrange
    responses: List[Tuple[int, dict[str, Any]] | Exception] = [
        httpx.TimeoutException("timeout"),
        (200, {"studies": [], "nextPageToken": None}),
    ]
    transport = MockStatefulTransport(responses)
    client = APIClient()
    client.client = httpx.Client(transport=transport)

    # Act
    client._fetch_page(params={})

    # Assert
    assert transport.call_count == 2


@pytest.mark.parametrize(
    "non_retryable_status_code",
    [
        400,  # Bad Request
        401,  # Unauthorized
        404,  # Not Found
    ],
)
def test_fetch_page_does_not_retry_on_non_retryable_errors(
    non_retryable_status_code: int,
) -> None:
    # Arrange
    # Set retries to 1 to ensure the test fails fast if retry logic is wrong
    settings.api.max_retries = 1
    responses: List[Tuple[int, dict[str, Any]] | Exception] = [
        (non_retryable_status_code, {"error": "client error"})
    ]
    transport = MockStatefulTransport(responses)
    client = APIClient()
    client.client = httpx.Client(transport=transport)

    # Act & Assert
    with pytest.raises(httpx.HTTPStatusError) as excinfo:
        client._fetch_page(params={})

    # Check that the raised exception has the expected status code
    assert excinfo.value.response.status_code == non_retryable_status_code
    # Crucially, assert that the request was only made once
    assert transport.call_count == 1


import tenacity
from tenacity import stop_after_attempt

def test_fetch_page_gives_up_after_max_attempts() -> None:
    # Arrange
    max_attempts = 3
    responses: List[Tuple[int, dict[str, Any]] | Exception] = [
        httpx.TimeoutException(f"timeout {i+1}") for i in range(max_attempts)
    ]
    transport = MockStatefulTransport(responses)
    client = APIClient()
    client.client = httpx.Client(transport=transport)

    # The @retry decorator reads settings at import time, so we
    # patch the retry object on the function directly for this test.
    client._fetch_page.retry.stop = stop_after_attempt(max_attempts)

    # Act & Assert
    with pytest.raises(tenacity.RetryError):
        client._fetch_page(params={})

    # stop_after_attempt(N) will try N times in total.
    assert transport.call_count == max_attempts


def test_fetch_page_retries_on_connect_error() -> None:
    # Arrange
    responses: List[Tuple[int, dict[str, Any]] | Exception] = [
        httpx.ConnectError("connection failed"),
        (200, {"studies": [], "nextPageToken": None}),
    ]
    transport = MockStatefulTransport(responses)
    client = APIClient()
    client.client = httpx.Client(transport=transport)

    # Act
    client._fetch_page(params={})

    # Assert
    assert transport.call_count == 2

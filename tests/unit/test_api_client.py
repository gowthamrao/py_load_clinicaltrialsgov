import httpx
import pytest
from datetime import datetime
from py_load_clinicaltrialsgov.extractor.api_client import APIClient

@pytest.fixture
def mock_transport():
    def handler(request: httpx.Request):
        if "pageToken=next" in str(request.url):
            return httpx.Response(200, json={"studies": [{"protocolSection": {"identificationModule": {"nctId": "NCT00000002"}}, "derivedSection": {}, "hasResults": False}], "nextPageToken": None})

        if "filter.advanced" in str(request.url):
            return httpx.Response(200, json={"studies": [{"protocolSection": {"identificationModule": {"nctId": "NCT00000003"}}, "derivedSection": {}, "hasResults": False}], "nextPageToken": None})

        return httpx.Response(200, json={"studies": [{"protocolSection": {"identificationModule": {"nctId": "NCT00000001"}}, "derivedSection": {}, "hasResults": False}], "nextPageToken": "next"})

    return httpx.MockTransport(handler)

def test_get_all_studies_pagination(mock_transport):
    client = APIClient()
    client.client = httpx.Client(transport=mock_transport)

    studies = list(client.get_all_studies())

    assert len(studies) == 2

def test_get_all_studies_delta_load(mock_transport):
    client = APIClient()
    client.client = httpx.Client(transport=mock_transport)

    studies = list(client.get_all_studies(updated_since=datetime(2023, 1, 1)))

    assert len(studies) == 1
    assert studies[0].protocol_section.identification_module['nctId'] == "NCT00000003"

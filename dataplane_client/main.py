import pytest
from unittest.mock import patch, Mock
import requests
from data_plane_server.data_plane_server import _log_and_abort
from dataplane.data_plane_utils import InvalidDataException
from dataplane_client.client import DataPlaneClient


class MockResponse:

    @staticmethod
    def json():
        return {"mock_key": "mock_response"}


@pytest.fixture
def mock_response_get(monkeypatch):
    def mock_get(*args, **kwargs):
        return MockResponse()

    monkeypatch.setattr(requests, "get", mock_get)


@pytest.fixture
def mock_response_post(monkeypatch):
    def mock_post(*args, **kwargs):
        return MockResponse()

    monkeypatch.setattr(requests, "post", mock_post)


def test_get_all_values(mock_response_get):
    client = DataPlaneClient("http://example.com")
    response = client.get_all_values("table_name", "column_name")
    assert response == {"mock_key": "mock_response"}


def test_get_filtered_rows(mock_response_post):
    client = DataPlaneClient("http://example.com")
    filter_spec = {"operator": "NONE"}
    response = client.get_filtered_rows("table_name", filter_spec, "column_name")
    assert response == {"mock_key": "mock_response"}


def test_get_range_spec(mock_response_get):
    client = DataPlaneClient("http://example.com")
    response = client.get_range_spec("table_name", "column_name")
    assert response == {"mock_key": "mock_response"}


def test_get_tables(mock_response_get):
    client = DataPlaneClient("http://example.com")
    response = client.get_tables()
    assert response == {"mock_key": "mock_response"}


def test_get_table_spec(mock_response_get):
    client = DataPlaneClient("http://example.com")
    response = client.get_table_spec("table_name")
    assert response == {"mock_key": "mock_response"}

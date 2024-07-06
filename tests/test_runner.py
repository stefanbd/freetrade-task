import io
import os
import sys
from io import BytesIO
from unittest.mock import MagicMock, patch

from google.cloud.exceptions import GoogleCloudError

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))


import pytest
from app.runner import ApiRunner
from requests.exceptions import RequestException


@pytest.fixture
def runner():
    return ApiRunner()


def test_api_call_success(runner, mocker):
    # Arrange
    mock_response = mocker.Mock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {"key": "value"}

    mocker.patch.object(runner.session, "get", return_value=mock_response)

    # Act
    response = runner._api_call("http://fakeapi.com", {"param": "value"})

    # Assert
    runner.session.get.assert_called_once_with(
        "http://fakeapi.com", params={"param": "value"}
    )
    assert response == {"key": "value"}


def test_api_call_failure(runner, mocker):
    # Arrange
    mock_response = mocker.Mock()
    mock_response.raise_for_status.side_effect = RequestException("API request failed")
    mocker.patch.object(runner.session, "get", return_value=mock_response)

    # Act & Assert
    with pytest.raises(RequestException):
        runner._api_call("http://fakeapi.com")


def test_list_users(runner, mocker):
    # Arrange
    BATCH_SIZE = 4
    RECORDS_LIMIT = 2

    mock_responses = [
        {"data": [{"id": 1}, {"id": 2}]},
        {"data": [{"id": 3}, {"id": 4}]},
    ]

    def mock_api_call(url, params):
        return mock_responses.pop(0)

    mocker.patch.object(runner, "_api_call", side_effect=mock_api_call)

    with patch("app.runner.BATCH_SIZE", BATCH_SIZE), patch(
        "app.runner.RECORDS_LIMIT", RECORDS_LIMIT
    ):
        # Act
        users = runner.list_users()

        # Assert
        assert len(users) == 4
        assert users == [{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}]


def test_upload_to_gcs_success(runner, mocker):
    # Arrange
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_bucket.blob.return_value = mock_blob
    mock_client = MagicMock()
    mock_client.bucket.return_value = mock_bucket
    mocker.patch.object(runner, "storage_client", mock_client)

    buffer = BytesIO(b"Example data")
    bucket_name = "test-bucket"
    destination_blob_name = "test-blob"

    # Act
    runner.upload_to_gcs(bucket_name, destination_blob_name, buffer)

    # Assert
    mock_client.bucket.assert_called_once_with(bucket_name)
    mock_bucket.blob.assert_called_once_with(destination_blob_name)
    mock_blob.upload_from_file.assert_called_once_with(
        buffer, content_type="application/vnd.apache.parquet"
    )


def test_upload_to_gcs_failure(runner, mocker):
    # Arrange
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_bucket.blob.return_value = mock_blob
    mock_client = MagicMock()
    mock_client.bucket.return_value = mock_bucket
    mocker.patch.object(
        runner.storage_client.__class__, "bucket", return_value=mock_bucket
    )
    mock_blob.upload_from_file.side_effect = GoogleCloudError("Upload failed")

    buffer = io.BytesIO(b"Example data")
    bucket_name = "test-bucket"
    destination_blob_name = "test-blob"

    # Act & Assert
    with pytest.raises(Exception) as excinfo:
        runner.upload_to_gcs(bucket_name, destination_blob_name, buffer)

    assert "Failed to upload file with error:" in str(excinfo.value)


def test_process_users(runner):
    # Arrange
    users = [{"id": 1}, {"id": 2}]

    # Act
    table = runner.process_users(users)

    # Assert
    assert table.num_rows == 2
    assert "extraction_timestamp" in table.schema.names
    assert table.column("id")[1].as_py() == 2


def test_execute(runner, mocker):
    # Arrange
    mock_list_users = mocker.patch.object(
        runner, "list_users", return_value=[{"id": 1}, {"id": 2}]
    )
    mock_upload_to_gcs = mocker.patch.object(runner, "upload_to_gcs")

    # Act
    runner.execute()

    # Assert
    mock_list_users.assert_called_once()
    mock_upload_to_gcs.assert_called_once()
    assert "extraction_timestamp" in mock_list_users.return_value[0]

import concurrent.futures
import io
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import pyarrow as pa
import pyarrow.parquet as pq
import requests
from app.settings.settings import (
    API_URL,
    BATCH_SIZE,
    BUCKET_NAME,
    BUCKET_PATH,
    RECORDS_LIMIT,
)
from google.cloud import storage
from google.cloud.exceptions import GoogleCloudError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class ApiRunner:
    def __init__(self):
        """Initialize the ApiRunner class with a requests session and a Google Cloud Storage client."""
        self.session = requests.Session()
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        self.session.mount("https://", HTTPAdapter(max_retries=retries))
        self.storage_client = storage.Client.create_anonymous_client()

    def _api_call(
        self, url: str, params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Make an API call to the specified URL and return the JSON response."""
        params = params or {}

        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise

    def list_users(self) -> List[Dict[str, Any]]:
        """Fetch user data from the API in batches."""
        all_users = []

        def fetch_batch(size: int) -> List[Dict[str, Any]]:
            params = {"_quantity": size}
            response = self._api_call(API_URL, params)
            return response.get("data", [])

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(fetch_batch, min(RECORDS_LIMIT, BATCH_SIZE - i))
                for i in range(0, BATCH_SIZE, RECORDS_LIMIT)
            ]

            for future in concurrent.futures.as_completed(futures):
                try:
                    all_users.extend(future.result())
                except Exception as e:
                    logger.error(f"An error occurred while fetching data: {e}")
                    raise

        return all_users

    def upload_to_gcs(
        self, bucket_name: str, destination_blob_name: str, buffer: io.BytesIO
    ) -> None:
        """Upload a file to Google Cloud Storage."""
        try:
            bucket = self.storage_client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)

            blob.upload_from_file(buffer, content_type="application/vnd.apache.parquet")
            logger.info(f"File uploaded to {bucket_name}/{destination_blob_name}.")
        except GoogleCloudError as e:
            raise Exception(f"Failed to upload file with error: {e}")

    def process_users(self, users: List[Dict[str, Any]]) -> pa.Table:
        """Process user data and convert to PyArrow Table."""
        current_timestamp = datetime.now().isoformat()
        for user in users:
            user["extraction_timestamp"] = current_timestamp
        return pa.Table.from_pylist(users)

    def execute(self):
        try:
            all_users = self.list_users()
            logger.info(f"Total amount of users after fetching: {len(all_users)}")

            table = self.process_users(all_users)

            buffer = io.BytesIO()
            pq.write_table(table, buffer, compression="snappy")
            buffer.seek(0)

            self.upload_to_gcs(BUCKET_NAME, BUCKET_PATH, buffer)
        except Exception as e:
            logger.error(f"An error occurred: {e}")
            raise

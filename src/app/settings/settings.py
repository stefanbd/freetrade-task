from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent

API_URL = "https://fakerapi.it/api/v1/users"
RECORDS_LIMIT = 1000

BATCH_SIZE = 100
BUCKET_NAME = "freetrade-data-eng-hiring"
BUCKET_PATH = "stefan/data_engineering_task.parquet"

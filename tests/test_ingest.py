import pytest
from unittest.mock import patch, MagicMock
from src.etl_pipeline.tasks.ingest_data import IngestData

@patch("src.etl_pipeline.tasks.ingest_data.get_s3_client")
def test_ingest_success(mock_get_s3_client, tmp_path):
    # Setup fake file
    file_path = tmp_path / "dummy.json"
    file_path.write_text('{"event": "test"}')

    # Mock S3
    mock_s3 = MagicMock()
    mock_get_s3_client.return_value = mock_s3
    mock_s3.list_buckets.return_value = {"Buckets": []}

    # Patch config
    with patch("src.etl_pipeline.tasks.ingest_data.load_config") as mock_config:
        mock_config.return_value = {
            "MINIO_BUCKET": "test-bucket",
            "RAW_LOCAL_FILE": str(file_path)
        }

        ingest = IngestData()
        ingest.ingest_raw_data()

        mock_s3.create_bucket.assert_called_once()
        mock_s3.upload_file.assert_called_once_with(str(file_path), "test-bucket", ingest.object_key)

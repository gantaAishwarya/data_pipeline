import pytest
import pandas as pd
from io import BytesIO
from unittest.mock import patch, MagicMock
from src.etl_pipeline.tasks.transform_data import TransformData

RAW_JSON = """
[
    {
        "user_id": 1,
        "action_type": "click",
        "timestamp": "2025-07-15T10:15:30Z",
        "metadata": {
            "device": "mobile",
            "location": "Berlin"
        }
    },
    {
        "user_id": null,
        "action_type": "view",
        "timestamp": "2025-07-15T11:00:00Z",
        "metadata": {
            "device": "mobile",
            "location": "Berlin"
        }
        
    },
    {
        "user_id": 2,
        "action_type": null,
        "timestamp": "invalid_timestamp",
        "metadata": {
            "device": "mobile",
            "location": "Berlin"
        }
    },
    {
        "user_id": 3,
        "action_type": "scroll",
        "timestamp": "2025-07-15T12:30:00Z"
    }

]
"""

@pytest.fixture
def mock_config():
    return {
        "MINIO_BUCKET": "test-bucket"
    }

@patch("src.etl_pipeline.tasks.transform_data.get_s3_client")
@patch("src.etl_pipeline.tasks.transform_data.generate_s3_key")
@patch("src.etl_pipeline.tasks.transform_data.load_config")
def test_transform_success(mock_load_config, mock_generate_s3_key, mock_get_s3_client, mock_config):
    # Setup mocks
    mock_load_config.return_value = mock_config
    mock_generate_s3_key.side_effect = ["raw/key.json", "processed/key.json"]

    # Fake S3 client and data
    mock_s3 = MagicMock()
    mock_get_s3_client.return_value = mock_s3
    mock_s3.get_object.return_value = {
        "Body": BytesIO(RAW_JSON.encode("utf-8"))
    }

    # Instantiate and call transform
    transformer = TransformData()
    df = transformer.transform()

    # Assertions on transformed data
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert "device" in df.columns
    assert "location" in df.columns
    assert "timestamp" in df.columns
    assert df["timestamp"].iloc[0] == "2025-07-15T10:15:30Z"
    assert df.shape[0] == 2 

    # Columns flattened from metadata exist
    assert "device" in df.columns
    assert "location" in df.columns

    # Final DataFrame contains only expected columns
    expected_cols = {"user_id", "action_type", "timestamp", "device", "location"}
    assert expected_cols.issubset(set(df.columns))
    
    # Check individual row correctness
    row_1 = df[df["user_id"] == 1].iloc[0]
    assert row_1["device"] == "mobile"
    assert row_1["location"] == "Berlin"
    assert row_1["timestamp"] == "2025-07-15T10:15:30Z"

    row_2 = df[df["user_id"] == 3].iloc[0]
    assert pd.isna(row_2["device"])
    assert pd.isna(row_2["location"])
    assert row_2["timestamp"] == "2025-07-15T12:30:00Z"


@patch("src.etl_pipeline.tasks.transform_data.get_s3_client")
@patch("src.etl_pipeline.tasks.transform_data.generate_s3_key")
@patch("src.etl_pipeline.tasks.transform_data.load_config")
def test_save_to_json(mock_load_config, mock_generate_s3_key, mock_get_s3_client, mock_config):
    mock_load_config.return_value = mock_config
    mock_generate_s3_key.side_effect = ["raw/key.json", "processed/key.json"]
    
    mock_s3 = MagicMock()
    mock_get_s3_client.return_value = mock_s3

    df = pd.DataFrame([{
        "user_id": 1,
        "action_type": "click",
        "timestamp": "2025-07-15T10:15:30Z",
        "device": "mobile",
        "location": "Berlin"
    }])

    transformer = TransformData()
    transformer.save_to_json(df)

    # Ensure upload_fileobj was called
    assert mock_s3.upload_fileobj.call_count == 1

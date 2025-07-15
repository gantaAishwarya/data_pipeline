import os
import pytest
import pandas as pd
from io import BytesIO
from unittest.mock import patch, MagicMock, call

# Set required env vars before any imports that depend on them
os.environ["POSTGRES_USER"] = "test"
os.environ["POSTGRES_PASSWORD"] = "test"
os.environ["POSTGRES_HOST"] = "localhost"
os.environ["POSTGRES_PORT"] = "5432"
os.environ["POSTGRES_DB"] = "test_db"

from src.database.models.dim_users import DimUser
from src.database.models.dim_actions import DimAction
from src.database.models.fact_user_actions import FactUserAction
from src.etl_pipeline.tasks.load_data import LoadData

PROCESSED_JSON = """
[
    {
        "user_id": 1,
        "action_type": "click",
        "timestamp": "2025-07-15T10:15:30Z",
        "device": "mobile",
        "location": "Berlin"
    },
    {
        "user_id": 2,
        "action_type": "scroll",
        "timestamp": "2025-07-15T10:20:00Z",
        "device": "desktop",
        "location": "Hamburg"
    }
]
"""

def get_mocked_dataframe() -> pd.DataFrame:
    """Return DataFrame from sample processed JSON."""
    return pd.read_json(BytesIO(PROCESSED_JSON.encode("utf-8")))


@patch("src.etl_pipeline.tasks.load_data.SessionLocal")
@patch("src.etl_pipeline.tasks.load_data.run_data_quality_checks")
@patch("src.etl_pipeline.tasks.load_data.get_s3_client")
@patch("src.etl_pipeline.tasks.load_data.generate_s3_key")
@patch("src.etl_pipeline.tasks.load_data.load_config")
def test_load_data_to_postgres(
    mock_load_config,
    mock_generate_s3_key,
    mock_get_s3_client,
    mock_quality_check,
    mock_session_local,
) -> None:
    """Test that LoadData.load calls session methods and interacts with S3 as expected."""
    mock_config = {"MINIO_BUCKET": "test-bucket"}
    mock_load_config.return_value = mock_config
    mock_generate_s3_key.return_value = "processed/key.json"

    mock_s3 = MagicMock()
    mock_get_s3_client.return_value = mock_s3
    mock_s3.get_object.return_value = {"Body": BytesIO(PROCESSED_JSON.encode("utf-8"))}

    df = get_mocked_dataframe()
    mock_quality_check.return_value = df

    mock_session = MagicMock()
    mock_session_local.return_value.__enter__.return_value = mock_session
    mock_session.query().filter_by().first.side_effect = [None] * 6

    loader = LoadData()
    loader.load()

    assert mock_session.add.call_count >= 6, "Expected at least 6 session.add() calls"
    assert mock_session.commit.call_count == 1, "Expected session.commit() to be called once"
    mock_quality_check.assert_called_once()
    mock_s3.get_object.assert_called_once_with(Bucket="test-bucket", Key="processed/key.json")


@patch("src.etl_pipeline.tasks.load_data.SessionLocal")
@patch("src.etl_pipeline.tasks.load_data.run_data_quality_checks")
@patch("src.etl_pipeline.tasks.load_data.get_s3_client")
@patch("src.etl_pipeline.tasks.load_data.generate_s3_key")
@patch("src.etl_pipeline.tasks.load_data.load_config")
def test_load_data_values_inserted_correctly(
    mock_load_config,
    mock_generate_s3_key,
    mock_get_s3_client,
    mock_quality_check,
    mock_session_local,
) -> None:
    """Test that LoadData.load inserts correct DimUser, DimAction, and FactUserAction instances."""
    mock_config = {"MINIO_BUCKET": "test-bucket"}
    mock_load_config.return_value = mock_config
    mock_generate_s3_key.return_value = "processed/key.json"

    mock_s3 = MagicMock()
    mock_get_s3_client.return_value = mock_s3
    mock_s3.get_object.return_value = {"Body": BytesIO(PROCESSED_JSON.encode("utf-8"))}

    df = get_mocked_dataframe()
    mock_quality_check.return_value = df

    mock_session = MagicMock()
    mock_session_local.return_value.__enter__.return_value = mock_session
    mock_session.query().filter_by().first.side_effect = [None] * 6

    loader = LoadData()
    loader.load()

    assert mock_session.add.call_count == 6, "Expected exactly 6 add() calls (2 users, 2 actions, 2 facts)"

    # Extract calls by ORM class type
    dim_users = [c[0][0] for c in mock_session.add.call_args_list if isinstance(c[0][0], DimUser)]
    dim_actions = [c[0][0] for c in mock_session.add.call_args_list if isinstance(c[0][0], DimAction)]
    fact_actions = [c[0][0] for c in mock_session.add.call_args_list if isinstance(c[0][0], FactUserAction)]

    # Check users data
    expected_users = [
        {"user_id": 1, "device": "mobile", "location": "Berlin"},
        {"user_id": 2, "device": "desktop", "location": "Hamburg"},
    ]
    for user_obj, expected in zip(dim_users, expected_users):
        assert user_obj.user_id == expected["user_id"]
        assert user_obj.device == expected["device"]
        assert user_obj.location == expected["location"]

    # Check actions data
    expected_actions = {"click", "scroll"}
    actual_actions = {a.action_type for a in dim_actions}
    assert actual_actions == expected_actions

    # Check fact data
    assert len(fact_actions) == 2
    for fact_obj, expected_user_id in zip(fact_actions, [1, 2]):
        assert fact_obj.user_id == expected_user_id
        # Timestamps should be datetime objects, convert to ISO string for comparison
        ts_str = fact_obj.timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
        # Match expected timestamps
        assert ts_str in ["2025-07-15T10:15:30Z", "2025-07-15T10:20:00Z"]

    mock_session.commit.assert_called_once()

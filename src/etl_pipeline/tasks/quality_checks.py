import pandas as pd
from src.config import logger

def run_data_quality_checks(df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Running basic data quality checks...")

        initial_count = len(df)
        summary = {}

        # Null check
        null_counts = df.isnull().sum()
        total_nulls = null_counts.sum()
        if total_nulls > 0:
            logger.warning(f"Null values detected:\n{null_counts[null_counts > 0]}")
            summary["nulls_found"] = int(total_nulls)
            df = df.dropna()
            logger.info(f"Dropped rows with nulls. Remaining rows: {len(df)}")

        # Duplicate check
        duplicate_count = df.duplicated().sum()
        if duplicate_count > 0:
            logger.warning(f"Found {duplicate_count} duplicate rows.")
            summary["duplicates_found"] = int(duplicate_count)
            df = df.drop_duplicates()
            logger.info(f"Removed duplicates. Remaining: {len(df)}")

        final_count = len(df)
        summary["rows_before"] = initial_count
        summary["rows_after"] = final_count
        logger.info(f"Data quality check complete. Rows before: {initial_count}, after: {final_count}")

        if summary:
            logger.debug(f"DQ Summary: {summary}")

        return df
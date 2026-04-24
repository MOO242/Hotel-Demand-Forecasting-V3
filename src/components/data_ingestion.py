"""
Data ingestion utilities for the Hotel Demand Forecasting project.

This module provides:
- UploadToPostgres: A utility class for uploading CSV files into PostgreSQL
    with logging and verification.
- DataLoader: A class for loading SQL tables into pandas DataFrames.

Both classes include structured logging, exception handling, and are designed
for production-grade ETL pipelines.
"""

import pandas as pd
from sqlalchemy import create_engine, text
from src.exception import CustomException
from src.logger import logger
import os
from dotenv import load_dotenv

load_dotenv()

# -------------------------------------------------------------------
# Database credentials and engine initialization
# -------------------------------------------------------------------

user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
host = os.getenv("POSTGRES_HOST")
db = os.getenv("POSTGRES_DB")
# port = os.getenv("POSTGRES_PORT")

engine = create_engine(f"postgresql://{user}:{password}@{host}/{db}")


class UploadToPostgres:
    """
    Upload a CSV file into a PostgreSQL table with logging and verification.
    """

    def __init__(self, csv_path: str, table_name: str, engine) -> None:
        self.csv_path = csv_path
        self.table_name = table_name
        self.engine = engine

    def run(self) -> None:
        """
        Execute the CSV upload process.

        """
        logger.info(f"Loading CSV from: {self.csv_path}")

        df = pd.read_csv(self.csv_path)
        logger.info(f"--- Step 2: Uploading {len(df)} rows to '{self.table_name}' ---")

        try:
            df.to_sql(self.table_name, self.engine, if_exists="append", index=False)
            logger.info("Upload successful!")

            self.verify_upload()
            logger.info("Verification successful!")

        except Exception as e:
            logger.error(CustomException(e))
            raise CustomException(e)

    def verify_upload(self) -> None:
        """
        Verify that the upload was successful by checking row count.

        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {self.table_name}"))
                count = result.scalar()

                logger.info("--- Step 3: Verification ---")
                logger.info(f"Rows found in PostgreSQL: {count}")

        except Exception as e:
            logger.error(CustomException(e))
            raise CustomException(e)


class DataLoader:
    """
    Load a PostgreSQL table into a pandas DataFrame.

    """

    def __init__(self, table_name: str, engine) -> None:
        self.table_name = table_name
        self.engine = engine

        logger.info(f"DataLoader initialized for table: {self.table_name}")

    def load(self) -> pd.DataFrame:
        """
        Load the SQL table into a pandas DataFrame.

        """
        try:
            logger.info(f"Loading data from table: {self.table_name}")
            query = f"SELECT * FROM {self.table_name}"
            df = pd.read_sql(query, self.engine)
            logger.info(f"Data loaded successfully. Shape: {df.shape}")

            return df

        except Exception as e:
            logger.error(CustomException(e))
            raise CustomException(e)


if __name__ == "__main__":
    uploader = UploadToPostgres(
        csv_path="/Hotel-Demand-Forecasting v3/Notebook/cleaned_hotel_data.csv",
        table_name="cleaned_hotel_data",
        engine=engine,
    )
    uploader.run()

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

engine = create_engine(f"postgresql://{user}:{password}@{host}/{db}")


class UploadToPostgres:
    """
    Upload a CSV file into a PostgreSQL table with logging and verification.

    This class:
    - Reads a CSV file into a pandas DataFrame
    - Uploads the DataFrame into a specified PostgreSQL table
    - Verifies the upload by checking row counts
    - Logs each step for traceability

    Parameters
    ----------
    csv_path : str
        Full path to the CSV file to upload.
    table_name : str
        Name of the PostgreSQL table to write into.
    engine : sqlalchemy.Engine
        SQLAlchemy engine connected to the target database.
    """

    def __init__(self, csv_path: str, table_name: str, engine) -> None:
        self.csv_path = csv_path
        self.table_name = table_name
        self.engine = engine

    def run(self) -> None:
        """
        Execute the CSV upload process.

        Steps:
        1. Load CSV into DataFrame
        2. Upload DataFrame to PostgreSQL
        3. Verify upload by counting rows

        Raises
        ------
        CustomException
            If any step in the upload process fails.
        """
        logger.info(f"Loading CSV from: {self.csv_path}")

        df = pd.read_csv(self.csv_path)
        logger.info(f"--- Step 2: Uploading {len(df)} rows to '{self.table_name}' ---")

        try:
            df.to_sql(self.table_name, self.engine, if_exists="replace", index=False)
            logger.info("Upload successful!")

            self.verify_upload()
            logger.info("Verification successful!")

        except Exception as e:
            logger.error(CustomException(e))
            raise CustomException(e)

    def verify_upload(self) -> None:
        """
        Verify that the upload was successful by checking row count.

        Logs the number of rows found in the target table.

        Raises
        ------
        CustomException
            If verification fails.
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

    This class is used throughout the RMS pipeline to retrieve
    cleaned or processed data directly from the database.

    Parameters
    ----------
    table_name : str
        Name of the SQL table to load.
    engine : sqlalchemy.Engine
        SQLAlchemy engine connected to the database.
    """

    def __init__(self, table_name: str, engine) -> None:
        self.table_name = table_name
        self.engine = engine

        logger.info(f"DataLoader initialized for table: {self.table_name}")

    def load(self) -> pd.DataFrame:
        """
        Load the SQL table into a pandas DataFrame.

        Returns
        -------
        pandas.DataFrame
            The loaded dataset.

        Raises
        ------
        CustomException
            If the SQL query or loading process fails.
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

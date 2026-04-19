import pandas as pd
import sys
from sqlalchemy import create_engine, text
from src.exception import CustomException
from src.logger import logger
import logging
import os
from dotenv import load_dotenv

load_dotenv()


# Database credentials
user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
host = os.getenv("POSTGRES_HOST")
db = os.getenv("POSTGRES_DB")
engine = create_engine(f"postgresql://{user}:{password}@{host}/{db}")


class UploadToPostgres:

    def __init__(self, csv_path, table_name, engine):
        """
        Upload a CSV file to PostgreSQL with logging and verification.
        """

        self.csv_path = csv_path
        self.table_name = table_name
        self.engine = engine

    def run(self):
        logger.info(f"Loading CSV from: {self.csv_path}")

        df = pd.read_csv(self.csv_path)
        logger.info(f"--- Step 2: Uploading {len(df)} rows to 'Resort_hotel' ---")

        # Attach engine to the class
        self.engine = engine

        try:
            df.to_sql("table_name", self.engine, if_exists="replace", index=False)
            logger.info("Upload successful!")

            self.verify_upload()
            logger.info("Verification successful!")

        except Exception as e:
            logger.error(CustomException(e))
            raise CustomException(e)

    def verify_upload(self):
        with self.engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM hotel_demand"))
            count = result.scalar()

            logger.info("--- Step 3: Verification ---")
            logger.info(f"Rows found in PostgreSQL: {count}")


class DataLoader:

    def __init__(self, path):
        self.path = path
        logger.info(f"DataLoader initialized with path: {self.path}")

    def load(self):

        try:
            logger.info(f"Loading data from: {self.path}")
            df = pd.read_csv(self.path)
            logger.info(f"Data loaded successfully. Shape: {df.shape}")

            return df
        except Exception as e:
            logger.error(CustomException(e))
            raise CustomException(e)


if __name__ == "__main__":
    uploader = UploadToPostgres(
        csv_path="/Hotel-Demand-Forecasting v3/Notebook/data.csv",
        table_name="Hotel_data",
        engine=engine,
    )
    uploader.run()

import pandas as pd
import sys
import matplotlib.pyplot as plt
import numpy as np
from sqlalchemy import create_engine, text
import logging
import os
from dotenv import load_dotenv

load_dotenv()


os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename="logs/data_loader.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

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
        logging.info(f"Loading CSV from: {self.csv_path}")

        df = pd.read_csv(self.csv_path)
        logging.info(f"--- Step 2: Uploading {len(df)} rows to 'Resort_hotel' ---")

        # Attach engine to the class
        self.engine = engine

        try:
            df.to_sql("table_name", self.engine, if_exists="replace", index=False)
            logging.info("Upload successful!")

            self.verify_upload()
            logging.info("Verification successful!")

        except Exception as e:
            logging.error(f"Error while loading data: {e}")
            raise

    def verify_upload(self):
        with self.engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM hotel_demand"))
            count = result.scalar()

            logging.info("--- Step 3: Verification ---")
            logging.info(f"Rows found in PostgreSQL: {count}")


class DataLoader:

    def __init__(self, path):
        self.path = path
        logging.info(f"DataLoader initialized with path: {self.path}")

    def load(self):

        try:
            logging.info(f"Loading data from: {self.path}")
            df = pd.read_csv(self.path)
            logging.info(f"Data loaded successfully. Shape: {df.shape}")

            return df
        except Exception as e:
            logging.error(f"Error while loading data: {e}")

            raise


if __name__ == "__main__":
    uploader = UploadToPostgres(
        csv_path="artifacts/Resort_hotel.csv", table_name="resort_hotel", engine=engine
    )
    uploader.run()

"""
Data preparation and KPI export pipeline for the Hotel Demand Forecasting project.

This script performs the following tasks:
- Loads cleaned hotel data from PostgreSQL
- Splits the dataset into Resort and City hotel subsets
- Saves both subsets as CSV artifacts for ML usage
- Uploads both subsets into separate SQL tables for analysis
- Computes KPI summary for a given year
- Saves KPI summary as a JSON artifact
"""

import json
import pandas as pd
from src.exception import CustomException
from src.logger import logger
from src.components.data_ingestion import DataLoader, engine
from src.components.kpi_engine import HotelKPIEngine

# -------------------------------------------------------------------
# Load data from SQL
# -------------------------------------------------------------------

loader = DataLoader("cleaned_hotel_data", engine)
df = loader.load()

# Split data by hotel type
resort_hotel = df[df["hotel"] == "Resort Hotel"]
city_hotel = df[df["hotel"] == "City Hotel"]

# -------------------------------------------------------------------
# Save CSV artifacts
# -------------------------------------------------------------------

try:
    resort_hotel.to_csv(
        r"D:/Hotel-Demand-Forecasting v3/artifacts/resort_hotel.csv", index=False
    )
    city_hotel.to_csv(
        r"D:/Hotel-Demand-Forecasting v3/artifacts/city_hotel.csv", index=False
    )
    logger.info("Hotel data successfully split into CSV files.")
except Exception as e:
    logger.error(CustomException(e))
    raise CustomException(e)

# -------------------------------------------------------------------
# Save SQL tables
# -------------------------------------------------------------------

try:
    resort_hotel.to_sql("resort_hotel", engine, if_exists="replace", index=False)
    city_hotel.to_sql("city_hotel", engine, if_exists="replace", index=False)
    logger.info("Hotel data successfully uploaded into SQL tables.")
except Exception as e:
    logger.error(CustomException(e))
    raise CustomException(e)

# -------------------------------------------------------------------
# KPI Functions
# -------------------------------------------------------------------


def KpiBy_hotel(data: pd.DataFrame, hotel_name: str, capacity: int, year: int) -> dict:
    """
    Calculate KPI summary for a specific hotel and year.
    """

    KPI = HotelKPIEngine(data, hotel_name, capacity)
    return KPI.summary(year)


def Kpi_to_file_json(filename: str, summary: dict) -> None:
    """
    Save KPI summary dictionary to a JSON file.

    """
    with open(filename, "w") as f:
        json.dump(summary, f, indent=4, default=str)


# -------------------------------------------------------------------
# Generate KPI summary and save as JSON
# -------------------------------------------------------------------

summary_year = KpiBy_hotel(df, "City_hotel", 300, 2016)

Kpi_to_file_json(
    r"D:/Hotel-Demand-Forecasting v3/artifacts/kpi_summary.json", summary_year
)

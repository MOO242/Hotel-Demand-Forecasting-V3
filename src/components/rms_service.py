import pandas as pd
from src.components.data_ingestion import DataLoader, engine
from src.components.kpi_engine import HotelKPIEngine
from src.logger import logger
from src.exception import CustomException


class RMSService:
    """
    Service layer that orchestrates data loading, filtering, and KPI calculations.
    """

    def __init__(self, engine):
        self.loader = DataLoader("cleaned_hotel_data", engine)

    # ---------------------------------------------------------
    # DATA ACCESS
    # ---------------------------------------------------------

    def load_all_data(self) -> pd.DataFrame:
        """Load the full dataset from SQL."""
        try:
            df = self.loader.load()
            return df
        except Exception as e:
            logger.error(CustomException(e))
            raise CustomException(e)

    def load_hotel(self, hotel_name: str) -> pd.DataFrame:
        """
        Load data for a specific hotel or all hotels.
        """
        df = self.load_all_data()

        if hotel_name == "Hotel list":
            return df

        return df[df["hotel"] == hotel_name]

    # ---------------------------------------------------------
    # KPI
    # ---------------------------------------------------------

    def get_kpi(self, hotel_name: str, year: int, capacity: int) -> dict:
        """
        Generate KPI summary for a hotel and year.
        """
        try:
            df = self.load_hotel(hotel_name)
            kpi = HotelKPIEngine(df, hotel_name, capacity)
            return kpi.summary(year)
        except Exception as e:
            logger.error(CustomException(e))
            raise CustomException(e)

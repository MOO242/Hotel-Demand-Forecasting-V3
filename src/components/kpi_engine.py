import pandas as pd
from src.exception import CustomException
from src.logger import logger


class HotelKPIEngine:
    """
    A production-grade KPI engine for hotel performance analytics.

    This class calculates core hospitality KPIs such as:
    - Rooms Available
    - Rooms Sold
    - Room Revenue
    - Occupancy %
    - RevPAR
    - ADR
    - Segmentation (market segment revenue)
    - Distribution channel revenue
    - Customer type revenue
    - Cancellation %

    Parameters
    ----------
    df : pandas.DataFrame
        The full hotel dataset.
    hotel_name : str
        Name of the hotel or hotel group being analyzed.
    capacity : int
        Total number of rooms in the hotel (used for Rooms Available).
    """

    def __init__(self, df, hotel_name, capacity):
        self.df = df.copy()
        self.hotel_name = hotel_name
        self.capacity = capacity

        # Precompute room nights
        self.df["room_nights"] = (
            self.df["stays_in_week_nights"] + self.df["stays_in_weekend_nights"]
        )

        # Ensure datetime format
        self.df["reservation_status_date"] = pd.to_datetime(
            self.df["reservation_status_date"]
        )

        logger.info(f"Initializing KPI Engine for: {hotel_name}")

    def filter_year(self, year):
        """
        Filter the dataset for a specific calendar year.

        Parameters
        ----------
        year : int
            The year to filter (e.g., 2016).

        Returns
        -------
        pandas.DataFrame
            Filtered dataframe containing only records from the given year.
        """
        start = pd.to_datetime(f"{year}-01-01")
        end = pd.to_datetime(f"{year}-12-31")

        logger.info(f"Filtering data for year: {year}")

        return self.df[
            (self.df["reservation_status_date"] >= start)
            & (self.df["reservation_status_date"] <= end)
        ]

    def Rooms_Available(self, df_year):
        """
        Calculate total rooms available for the selected year.

        Formula:
            Rooms Available = Number of unique dates × hotel capacity

        Parameters
        ----------
        df_year : pandas.DataFrame
            Year-filtered dataset.

        Returns
        -------
        int
            Total rooms available for the year.
        """
        try:
            return df_year["reservation_status_date"].nunique() * self.capacity
        except Exception as e:
            logger.error(CustomException(e))
            raise CustomException(e)

    def Rooms_sold(self, df_year):
        """
        Calculate total room nights sold (excluding cancellations).

        Parameters
        ----------
        df_year : pandas.DataFrame

        Returns
        -------
        int
            Total room nights sold.
        """
        try:
            active_df = df_year[df_year["is_canceled"] == 0]
            return active_df["room_nights"].sum()
        except Exception as e:
            logger.error(CustomException(e))
            raise CustomException(e)

    def Room_revenue(self, df_year):
        """
        Calculate total room revenue for the year.

        Formula:
            Room Revenue = ADR × Room Nights Sold

        Parameters
        ----------
        df_year : pandas.DataFrame

        Returns
        -------
        float
            Total room revenue.
        """
        try:
            active_df = df_year[df_year["is_canceled"] == 0].copy()
            active_df["room_revenue"] = active_df["adr"] * active_df["room_nights"]
            return active_df["room_revenue"].sum()
        except Exception as e:
            logger.error(CustomException(e))
            raise CustomException(e)

    def occupancy(self, rooms_sold, rooms_available):
        """
        Calculate occupancy percentage.

        Formula:
            Occupancy % = (Rooms Sold / Rooms Available) × 100

        Returns
        -------
        float
            Occupancy percentage.
        """
        try:
            return (rooms_sold / rooms_available) * 100
        except Exception as e:
            logger.error(CustomException(e))
            raise CustomException(e)

    def RevPAR(self, room_revenue, room_available):
        """
        Calculate Revenue Per Available Room (RevPAR).

        Formula:
            RevPAR = Room Revenue / Rooms Available

        Returns
        -------
        float
            RevPAR value.
        """
        try:
            return room_revenue / room_available
        except Exception as e:
            logger.error(CustomException(e))
            raise CustomException(e)

    def ADR(self, room_revenue, room_sold):
        """
        Calculate Average Daily Rate (ADR).

        Formula:
            ADR = Room Revenue / Rooms Sold

        Returns
        -------
        float
            ADR value.
        """
        try:
            return room_revenue / room_sold
        except Exception as e:
            logger.error(CustomException(e))
            raise CustomException(e)

    def segmentation(self, df_year):
        """
        Calculate revenue contribution by market segment.

        Returns
        -------
        pandas.Series
            Revenue grouped by market segment.
        """
        try:
            active_df = df_year[df_year["is_canceled"] == 0].copy()
            active_df["room_revenue"] = active_df["adr"] * active_df["room_nights"]
            return active_df.groupby("market_segment")["room_revenue"].sum()
        except Exception as e:
            logger.error(CustomException(e))
            raise CustomException(e)

    def distribution_channel(self, df_year):
        """
        Calculate revenue contribution by distribution channel.

        Returns
        -------
        pandas.Series
            Revenue grouped by distribution channel.
        """
        try:
            active_df = df_year[df_year["is_canceled"] == 0].copy()
            active_df["room_revenue"] = active_df["adr"] * active_df["room_nights"]
            return active_df.groupby("distribution_channel")["room_revenue"].sum()
        except Exception as e:
            logger.error(CustomException(e))
            raise CustomException(e)

    def customer_type(self, df_year):
        """
        Calculate revenue contribution by customer type.

        Returns
        -------
        pandas.Series
            Revenue grouped by customer type.
        """
        try:
            active_df = df_year[df_year["is_canceled"] == 0].copy()
            active_df["room_revenue"] = active_df["adr"] * active_df["room_nights"]
            return active_df.groupby("customer_type")["room_revenue"].sum()
        except Exception as e:
            logger.error(CustomException(e))
            raise CustomException(e)

    def cancellation(self, df_year):
        """
        Calculate cancellation percentage.

        Formula:
            Cancellation % = (Canceled Bookings / Total Bookings) × 100

        Returns
        -------
        float
            Cancellation percentage.
        """
        try:
            total = len(df_year)
            canceled = df_year[df_year["is_canceled"] == 1].shape[0]
            return (canceled / total) * 100
        except Exception as e:
            logger.error(CustomException(e))
            raise CustomException(e)

    def summary(self, year):
        """
        Generate a full KPI summary for the selected year.

        Returns
        -------
        dict
            Dictionary containing all KPIs and breakdowns.
        """
        try:
            df_year = self.filter_year(year)
            room_available = self.Rooms_Available(df_year)
            rooms_sold = self.Rooms_sold(df_year)
            room_revenue = self.Room_revenue(df_year)

            return {
                "Hotel code": self.hotel_name,
                "Year": year,
                "room_available": room_available,
                "rooms_sold": rooms_sold,
                "room_revenue": f"{room_revenue:,.2f} $",
                "Occupancy": f"{self.occupancy(rooms_sold, room_available):.2f}%",
                "RevPAR": f"{self.RevPAR(room_revenue, room_available):.2f} $",
                "ADR": f"{self.ADR(room_revenue, rooms_sold):.2f} $",
                "Segmentation": self.segmentation(df_year),
                "Distribution": self.distribution_channel(df_year),
                "Customer": self.customer_type(df_year),
                "Cancellation": self.cancellation(df_year),
            }

        except Exception as e:
            logger.error(CustomException(e))
            raise CustomException(e)

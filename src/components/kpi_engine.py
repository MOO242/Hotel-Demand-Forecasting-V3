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

        self.df["stay_date"] = pd.to_datetime(
            self.df["arrival_date_year"].astype(str)
            + "-"
            + self.df["arrival_date_month"]
            + "-"
            + self.df["arrival_date_day_of_month"].astype(str)
        )

        logger.info(f"Initializing KPI Engine for: {hotel_name}")

    def filter_year(self, year):
        """
        Filter the dataset for a specific calendar year.

        """
        logger.info(f"Filtering data for year: {year}")

        return self.df[self.df["arrival_date_year"] == year]

    def rooms_available(self, df_year):
        """
        Calculate total rooms available for the selected year.
        """
        try:
            total_days = df_year["stay_date"].nunique()
            return total_days * self.capacity
        except Exception as e:
            logger.error(CustomException(e))
            raise CustomException(e)

    def rooms_sold(self, df_year):
        """
        Calculate total room nights sold (excluding cancellations).

        """
        try:
            active_df = df_year[df_year["is_canceled"] == 0]
            return active_df["room_nights"].sum()
        except Exception as e:
            logger.error(CustomException(e))
            raise CustomException(e)

    def room_revenue(self, df_year):
        """
        Calculate total room revenue for the year.

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
        """
        try:
            return (rooms_sold / rooms_available) * 100
        except Exception as e:
            logger.error(CustomException(e))
            raise CustomException(e)

    def RevPAR(self, room_revenue, room_available):
        """
        Calculate Revenue Per Available Room (RevPAR).
        """
        try:
            RevPAR = room_revenue / room_available
            return RevPAR
        except Exception as e:
            logger.error(CustomException(e))
            raise CustomException(e)

    def ADR(self, room_revenue, room_sold):
        """
        Calculate Average Daily Rate (ADR).
        """
        try:
            ADR = room_revenue / room_sold
            return ADR
        except Exception as e:
            logger.error(CustomException(e))
            raise CustomException(e)

    def segmentation(self, df_year):
        """
        Calculate revenue contribution by market segment.

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
        Returns a clean Pandas DataFrame ready for analysis.
        """
        try:
            df_year = self.filter_year(year)
            room_available = self.rooms_available(df_year)
            rooms_sold = self.rooms_sold(df_year)
            room_revenue = self.room_revenue(df_year)

            summary_dict = {
                "hotel_code": self.hotel_name,
                "year": year,
                "rooms_available": room_available,
                "rooms_sold": rooms_sold,
                "room_revenue": room_revenue,
                "occupancy": self.occupancy(rooms_sold, room_available),
                "revpar": self.RevPAR(room_revenue, room_available),
                "adr": self.ADR(room_revenue, rooms_sold),
                "cancellation_rate": self.cancellation(df_year),
            }

            return pd.DataFrame([summary_dict])

        except Exception as e:
            logger.error(CustomException(e))
            raise CustomException(e)

    def daily_kpi(self, year):
        df_year = self.filter_year(year).copy()

        # 1. Calculate room nights (rooms sold per reservation)
        df_year["room_nights"] = (
            df_year["stays_in_week_nights"] + df_year["stays_in_weekend_nights"]
        )

        # 2. Remove canceled bookings (they do NOT generate revenue)
        df_year = df_year[df_year["is_canceled"] == 0]

        # 3. Daily rooms sold = sum of room nights per day
        daily = (
            df_year.groupby("reservation_status_date")
            .agg(rooms_sold=("room_nights", "sum"), adr=("adr", "mean"))  # ADR per day
            .reset_index()
        )

        # 4. Daily rooms available = hotel capacity
        daily["rooms_available"] = self.capacity

        # 5. Daily room revenue
        daily["room_revenue"] = daily["rooms_sold"] * daily["adr"]

        # 6. Daily KPIs
        daily["revpar"] = daily["room_revenue"] / daily["rooms_available"]
        daily["occupancy"] = daily["rooms_sold"] / daily["rooms_available"]

        return daily[
            [
                "reservation_status_date",
                "rooms_available",
                "rooms_sold",
                "room_revenue",
                "revpar",
                "adr",
                "occupancy",
            ]
        ]

import pandas as pd
from src.exception import CustomException
from src.logger import logger

import logging
from src.components.data_ingestion import DataLoader

loader = DataLoader("Notebook/data.csv")
df = loader.load()

# I split the data and placed into artifacts folder for ML model.

Resort_hotel = df[df["hotel"] == "Resort Hotel"]
City_hotel = df[df["hotel"] == "City Hotel"]


class HotelKPIEngine:

    def __init__(self, df, hotel_name, capacity):

        self.df = df.copy()
        self.hotel_name = hotel_name
        self.capacity = capacity

        self.df["room_nights"] = (
            self.df["stays_in_week_nights"] + self.df["stays_in_weekend_nights"]
        )

        self.df["reservation_status_date"] = pd.to_datetime(
            self.df["reservation_status_date"]
        )

        logger.info(f"Initializing KPI Engine for: {hotel_name}")

    def filter_year(self, year):

        start = pd.to_datetime(f"{year}-01-01")
        end = pd.to_datetime(f"{year}-12-31")

        logger.info(f"Filtering data for year: {year}")

        return self.df[
            (self.df["reservation_status_date"] >= start)
            & (self.df["reservation_status_date"] <= end)
        ]

    def Rooms_Available(self, df_year):
        try:
            logger.info(
                f"Room Available calculated successfully for hotel: {self.hotel_name}"
            )
            return df_year["reservation_status_date"].nunique() * self.capacity
        except Exception as e:
            logger.error(CustomException(e))
            raise CustomException(e)

    def Rooms_sold(self, df_year):
        try:
            active_df = df_year[df_year["is_canceled"] == 0].copy()
            logger.info(
                f"Room sold calculated successfully for hotel: {self.hotel_name}"
            )
            return active_df["room_nights"].sum()
        except Exception as e:
            logger.error(CustomException(e))
            raise CustomException(e)

    def Room_revenue(self, df_year):
        try:
            active_df = df_year[df_year["is_canceled"] == 0].copy()
            active_df["room_revenue"] = active_df["adr"] * active_df["room_nights"]
            logger.info(
                f"Room revenue calculated successfully for hotel: {self.hotel_name}"
            )
            return active_df["room_revenue"].sum()
        except Exception as e:
            logger.error(CustomException(e))
            raise CustomException(e)

    def occupancy(self, rooms_sold, rooms_Available):
        try:
            logger.info(
                f"Room occupancy calculated successfully for hotel: {self.hotel_name}"
            )
            return (rooms_sold / rooms_Available) * 100
        except Exception as e:
            logger.error(CustomException(e))
            raise CustomException(e)

    def RevPAR(self, room_revenue, room_available):
        try:
            logger.info(f"RevPAR calculated successfully for hotel: {self.hotel_name}")
            return room_revenue / room_available
        except Exception as e:
            logger.error(CustomException(e))
            raise CustomException(e)

    def ADR(self, room_revenue, room_sold):

        try:
            logger.info(f"ADR calculated successfully for hotel: {self.hotel_name}")
            return room_revenue / room_sold
        except Exception as e:
            logger.error(CustomException(e))
            raise CustomException(e)

    def segmentation(self, df_year):

        try:
            active_df = df_year[df_year["is_canceled"] == 0].copy()
            active_df["room_revenue"] = active_df["adr"] * active_df["room_nights"]

            segmentation = active_df.groupby("market_segment")["room_revenue"].sum()
            logger.info(
                f"segmentation calculated successfully for hotel: {self.hotel_name}"
            )
            return segmentation

        except Exception as e:
            logger.error(CustomException(e))
            raise CustomException(e)

    def distribution_channel(self, df_year):

        try:
            active_df = df_year[df_year["is_canceled"] == 0].copy()
            active_df["room_revenue"] = active_df["adr"] * active_df["room_nights"]

            Distribution = active_df.groupby("distribution_channel")[
                "room_revenue"
            ].sum()
            logger.info(
                f"segmentation calculated successfully for hotel: {self.hotel_name}"
            )
            return Distribution

        except Exception as e:
            logger.error(CustomException(e))
            raise CustomException(e)

    def customer_type(self, df_year):

        try:
            active_df = df_year[df_year["is_canceled"] == 0].copy()
            active_df["room_revenue"] = active_df["adr"] * active_df["room_nights"]

            Customer = active_df.groupby("customer_type")["room_revenue"].sum()
            logger.info(
                f"segmentation calculated successfully for hotel: {self.hotel_name}"
            )
            return Customer

        except Exception as e:
            logger.error(CustomException(e))
            raise CustomException(e)

    def cancellation(self, df_year):

        try:
            total = len(df_year)
            canceled = df_year[df_year["is_canceled"] == 1].shape[0]
            logger.info(
                f"cancellation calculated successfully for hotel: {self.hotel_name}"
            )
            return (canceled / total) * 100

        except Exception as e:
            logger.error(CustomException(e))
            raise CustomException(e)

    def summary(self, year):

        try:
            df_year = self.filter_year(year)
            room_available = self.Rooms_Available(df_year)
            rooms_sold = self.Rooms_sold(df_year)
            room_revenue = self.Room_revenue(df_year)
            Segments = self.segmentation(df_year)
            Distribution = self.distribution_channel(df_year)
            Customer = self.customer_type(df_year)
            Cancellation = self.cancellation(df_year)

            return {
                "Hotel code": self.hotel_name,
                "Year": year,
                "room_available": room_available,
                "rooms_sold": rooms_sold,
                "room_revenue": f"{room_revenue:,.2f} $",
                "Occupancy": f"{self.occupancy(rooms_sold, room_available):.2f}%",
                "RevPAR": f"{self.RevPAR(room_revenue, room_available):.2f} $",
                "ADR": f"{self.ADR(room_revenue, rooms_sold):.2f} $",
                "Segmentation": Segments,
                "Distribution": Distribution,
                "Customer": Customer,
                "Cancellation": Cancellation,
            }

        except Exception as e:
            logger.error(CustomException(e))
            raise CustomException(e)

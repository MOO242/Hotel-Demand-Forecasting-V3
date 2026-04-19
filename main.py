import pandas as pd
from src.exception import CustomException
from src.logger import logger
import logging
from src.components.data_ingestion import DataLoader
from src.components.kpi_engine import HotelKPIEngine

loader = DataLoader("Notebook/data.csv")
df = loader.load()

# I split the data and placed into artifacts folder for ML model.

Resort_hotel = df[df["hotel"] == "Resort Hotel"]
City_hotel = df[df["hotel"] == "City Hotel"]


KPI = HotelKPIEngine(City_hotel, "City hotel", 300)
print(KPI.summary(2016))

KPI = HotelKPIEngine(Resort_hotel, "Resort hotel", 200)
print(KPI.summary(2016))

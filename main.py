import pandas as pd
import json
from src.exception import CustomException
from src.logger import logger
import logging
from src.components.data_ingestion import DataLoader
from src.components.kpi_engine import HotelKPIEngine

loader = DataLoader("Notebook/cleaned_hotel_data.csv")
df = loader.load()

# I split the data and placed into artifacts folder for ML model.

# Resort_hotel = df[df["hotel"] == "Resort Hotel"]
# City_hotel = df[df["hotel"] == "City Hotel"]

# KPI = HotelKPIEngine(df, "Resort hotel", 200)
# print(KPI.summary(2016))


KPI = HotelKPIEngine(df, "Hotel list", 500)
print(KPI.summary(2016))


## To txt file

# with open("kpi_summary.txt", "w") as f:
#     f.write(str(KPI.summary(2016)))

## To JSON file
summary_2016 = KPI.summary(2016)

with open("kpi_summary.json", "w") as f:
    json.dump(summary_2016, f, indent=4, default=str)

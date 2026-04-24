from src.components.rms_service import RMSService
from src.components.data_ingestion import engine

service = RMSService(engine)

summary = service.get_kpi("City Hotel", 2016, 300)
print(summary)

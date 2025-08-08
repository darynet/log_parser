from pydantic import BaseModel, Field
from datetime import datetime


class MessageModel(BaseModel):
    motocycle_id: int = Field(..., description="Идентификатор мотоцикла")
    motocycle_model: str = Field(..., description="Модель мотоцикла")
    motocycle_number: str = Field(..., description="Номер мотоцикла")
    motocycle_speed: int = Field(..., description="Скорость мотоцикла")
    date: datetime = Field(..., description="Дата и время")

    class Config:
        json_encoders = {datetime: lambda dt: dt.strftime("%Y-%m-%dT%H:%M:%S")}

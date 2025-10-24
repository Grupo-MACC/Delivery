# Payment/app_payment/sql/schemas.py
from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, Literal

class Message(BaseModel):
    detail: Optional[str] = Field(example="error or success message")

DeliveryStatusType = Literal["Delivering", "Delivered", "Not Delivered"]

class DeliveryStatusBase(BaseModel):
    order_id: int = Field(description="Id del pedido", example=1)
    status: DeliveryStatusType = Field(description="Estado actual de la entrega", example="Delivered")

class DeliveryStatusCreate(DeliveryStatusBase):
    """Schema definition to create/update delivery status"""

class DeliveryStatus(DeliveryStatusBase):
    model_config = ConfigDict(from_attributes=True)
    id: int = Field(description="Primary key/identifier of the delivery status", example=1)

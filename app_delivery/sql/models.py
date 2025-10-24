# -*- coding: utf-8 -*-
"""Database models definitions. Table representations as class."""
from sqlalchemy import Column, Integer, String, UniqueConstraint
from microservice_chassis_grupo2.sql.models import BaseModel

class DeliveryStatus(BaseModel):
    """Tabla para guardar el estado de entrega de los pedidos"""
    __tablename__ = "delivery_status"
    __table_args__ = (UniqueConstraint("order_id", name="uq_delivery_order_id"),)

    id = Column(Integer, primary_key=True)
    order_id = Column(Integer, nullable=False)
    status = Column(String(32), nullable=False, default="Pending")
# -*- coding: utf-8 -*-
"""Database models definitions. Table representations as class."""
from sqlalchemy import Column, DateTime, Integer, String
from sqlalchemy.sql import func
from .database import Base  # igual que en Order: Base viene de database.py


class BaseModel(Base):
    """Base database table representation to reuse."""
    __abstract__ = True
    creation_date = Column(DateTime(timezone=True), server_default=func.now())
    update_date = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    def __repr__(self):
        fields = ""
        for column in self.__table__.columns:
            if fields == "":
                fields = f"{column.name}='{getattr(self, column.name)}'"
            else:
                fields = f"{fields}, {column.name}='{getattr(self, column.name)}'"
        return f"<{self.__class__.__name__}({fields})>"

    @staticmethod
    def list_as_dict(items):
        """Returns list of items as dict."""
        return [i.as_dict() for i in items]

    def as_dict(self):
        """Return the item as dict."""
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class Payment(BaseModel):
    """Payment database table representation (propia de payment-svc)."""
    STATUS_INITIATED  = "Initiated"
    STATUS_PAYED = "Paid"
    STATUS_FAILED     = "Failed"
    STATUS_CANCELED   = "Canceled"

    __tablename__ = "payment"

    id = Column(Integer, primary_key=True)
    # Nota: NO FK a 'manufacturing_order' (esa tabla vive en otro servicio)
    order_id = Column(Integer, nullable=False)

    # Importe en unidades mínimas (céntimos) para evitar coma flotante
    amount_minor = Column(Integer, nullable=False)
    currency = Column(String(3), nullable=False, default="EUR")

    status = Column(String(32), nullable=False, default=STATUS_INITIATED)

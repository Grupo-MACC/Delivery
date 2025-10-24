# -*- coding: utf-8 -*-
"""FastAPI router definitions."""
import logging
from microservice_chassis_grupo2.core.dependencies import get_db, get_current_user
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from sql import crud, schemas, database
logger = logging.getLogger(__name__)
router = APIRouter(
    prefix="/delivery"
)

@router.get(
    "/status/{order_id}",
    response_model=schemas.DeliveryStatus,
    summary="Get delivery status by order_id",
    tags=["Delivery"]
)
async def get_delivery_status(
    order_id: int,
    db: AsyncSession = Depends(get_db),
    user_id: int = Depends(get_current_user)
):
    """Obtiene el estado actual de entrega."""
    delivery_status = await crud.get_delivery_status(db, order_id)
    if not delivery_status:
        raise HTTPException(status_code=404, detail="Delivery status not found")
    return delivery_status
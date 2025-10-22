# -*- coding: utf-8 -*-
"""FastAPI router definitions."""
import asyncio
import logging
import httpx
from fastapi import APIRouter, Depends, HTTPException
from typing import List
from .router_utils import raise_and_log_error, ORDER_SERVICE_URL
from sqlalchemy.ext.asyncio import AsyncSession

from sql import crud, schemas, database
logger = logging.getLogger(__name__)
router = APIRouter()

@router.post("/deliver/{order_id}")
async def deliver(order_id: int):
    await asyncio.sleep(20)
    try:
        async with httpx.AsyncClient() as client:
            url = f"{ORDER_SERVICE_URL}/update_order_status/{order_id}"
            
            response = await client.put(
                url,
                params={"status": "Delivered"}
            )
            print(order_id,"order delivered")
    except httpx.HTTPError as exc:
        print(exc)
    except Exception as exc:
        print(exc)

@router.post("/delivery_status/{order_id}", response_model=schemas.DeliveryStatus)
async def create_or_update_delivery_status(order_id: int, status: str, db: AsyncSession = Depends(database.SessionLocal)):
    """Crea o actualiza el estado de entrega."""
    delivery_status = await crud.create_or_update_delivery_status(db, order_id, status)
    return delivery_status


@router.get("/delivery_status/{order_id}", response_model=schemas.DeliveryStatus)
async def get_delivery_status(order_id: int, db: AsyncSession = Depends(database.SessionLocal)):
    """Obtiene el estado actual de entrega."""
    delivery_status = await crud.get_delivery_status(db, order_id)
    if not delivery_status:
        raise HTTPException(status_code=404, detail="Delivery status not found")
    return delivery_status
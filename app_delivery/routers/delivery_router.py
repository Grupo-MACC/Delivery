# -*- coding: utf-8 -*-
"""FastAPI router definitions."""
import asyncio
import logging
import httpx
from fastapi import APIRouter
from typing import List
from .router_utils import raise_and_log_error, ORDER_SERVICE_URL

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

from sql import crud, models, schemas
from sqlalchemy.ext.asyncio import AsyncSession
import asyncio
import logging
from dependencies import get_db


logger = logging.getLogger(__name__)

async def deliver(order_id: int):
    await asyncio.sleep(20)
    try:
        logger.info(f"[DELIVERY] 📤 Pedido {order_id} publicado como entregado.")
        return "Delivered"
    except Exception as exc:
        logger.error(f"[DELIVERY] ❌ Error al publicar entrega del pedido {order_id}: {exc}")
        return "Not Delivered"

async def update_delivery_status(order_id: int, status: str):
    """Actualiza el estado de entrega de un pedido en la base de datos."""
    async for db in get_db():
        db_delivery = await crud.create_or_update_delivery_status(
            db=db,
            order_id=order_id,
            status=status
        )
    return db_delivery
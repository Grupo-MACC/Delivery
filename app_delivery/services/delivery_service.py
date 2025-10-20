import asyncio
import logging

logger = logging.getLogger(__name__)

async def deliver(order_id: int):
    await asyncio.sleep(20)
    try:
        logger.info(f"[DELIVERY] 📤 Pedido {order_id} publicado como entregado.")
    except Exception as exc:
        logger.error(f"[DELIVERY] ❌ Error al publicar entrega del pedido {order_id}: {exc}")

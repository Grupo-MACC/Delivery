import asyncio
import json
import logging
import httpx
from aio_pika import connect_robust, Message, ExchangeType
from broker.setup_rabbitmq import RABBITMQ_HOST, EXCHANGE_NAME
from services import delivery_service
logger = logging.getLogger(__name__)

async def consume_order_events():
    connection = await connect_robust(RABBITMQ_HOST)
    channel = await connection.channel()
    
    delivery_ready_queue = await channel.declare_queue("order_ready_queue", durable=True)
    
    await delivery_ready_queue.bind(EXCHANGE_NAME, routing_key="order.ready")


    await delivery_ready_queue.consume(handle_order_ready)

    logger.info("[ORDER] 🟢 Escuchando eventos de pago...")
    await asyncio.Future()

async def handle_order_ready(message):
    async with message.process():
        data = json.loads(message.body)
        order_id = data["order_id"]
        await delivery_service.deliver(order_id=order_id)  # ✅ Con await
        await publish_order_delivered(order_id=order_id)   # ✅ Con await
        logger.info(f"[ORDER] ✅ Pago confirmado para orden: {order_id}")
        
async def publish_order_delivered(order_id):
    connection = await connect_robust(RABBITMQ_HOST)
    channel = await connection.channel()
    exchange = await channel.declare_exchange(EXCHANGE_NAME, ExchangeType.TOPIC, durable=True)
    await exchange.publish(
        Message(body=json.dumps({"order_id": order_id}).encode()),
        routing_key="delivery.ready"
    )
    logger.info(f"[ORDER] 📤 Publicado evento order.created → {order_id}")
    await connection.close()
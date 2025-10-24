import asyncio
import json
import httpx
import logging
from microservice_chassis_grupo2.core.rabbitmq_core import get_channel, declare_exchange, PUBLIC_KEY_PATH
from aio_pika import Message
from services import delivery_service
from microservice_chassis_grupo2.core.router_utils import AUTH_SERVICE_URL

logger = logging.getLogger(__name__)

async def consume_order_events():
    _, channel = await get_channel()
    
    exchange = await declare_exchange(channel)
    
    order_ready_queue = await channel.declare_queue('order_ready_queue', durable=True)
    await order_ready_queue.bind(exchange, routing_key='order.ready')

    await order_ready_queue.consume(handle_order_events)

    logger.info("[ORDER] 🟢 Escuchando eventos de pago...")
    await asyncio.Future()

async def handle_order_events(message):
    async with message.process():
        data = json.loads(message.body)
        order_id = data["order_id"]
        status="Delivering"
        db_delivery= await delivery_service.update_delivery_status(order_id, status)
        await publish_order_delivered(order_id=order_id,status=status)

        status = await delivery_service.deliver(order_id=order_id)
        await delivery_service.update_delivery_status(order_id, status)

        await publish_order_delivered(order_id=order_id,status=status)
        logger.info(f"[ORDER] ✅ Pago confirmado para orden: {order_id}")

async def consume_auth_events():
    _, channel = await get_channel()
    
    exchange = await declare_exchange(channel)
    
    delivery_queue = await channel.declare_queue('delivery_queue', durable=True)
    await delivery_queue.bind(exchange, routing_key="auth.running")
    await delivery_queue.bind(exchange, routing_key="auth.not_running")
    
    await delivery_queue.consume(handle_auth_events)

async def handle_auth_events(message):
    async with message.process():
        data = json.loads(message.body)
        if data["status"] == "running":
            try:
                async with httpx.AsyncClient(
                    verify="/certs/ca.pem",
                    cert=("/certs/delivery/delivery-cert.pem", "/certs/delivery/delivery-key.pem"),
                ) as client:
                    response = await client.get(
                        f"{AUTH_SERVICE_URL}/auth/public-key"
                    )
                    response.raise_for_status()
                    public_key = response.text
                    
                    with open(PUBLIC_KEY_PATH, "w", encoding="utf-8") as f:
                        f.write(public_key)
                    
                    logger.info(f"✅ Clave pública de Auth guardada en {PUBLIC_KEY_PATH}")
            except Exception as exc:
                print(exc)

async def publish_order_delivered(order_id,status):
    connection, channel = await get_channel()
    
    exchange = await declare_exchange(channel)
    await exchange.publish(
        Message(body=json.dumps({"order_id": order_id, "status":status}).encode()),
        routing_key="delivery.ready"
    )
    logger.info(f"[ORDER] 📤 Publicado evento order.created → {order_id}")
    await connection.close()
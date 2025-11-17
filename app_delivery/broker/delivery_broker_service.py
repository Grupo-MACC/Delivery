import asyncio
import json
import httpx
import logging
from microservice_chassis_grupo2.core.rabbitmq_core import get_channel, declare_exchange, declare_exchange_logs, PUBLIC_KEY_PATH
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
    await publish_to_logger(
        message={"message": "🟢 Delivery escuchando eventos de order.ready"},
        topic="delivery.info",
    )
    await asyncio.Future()

async def handle_order_events(message):
    async with message.process():
        data = json.loads(message.body)
        order_id = data["order_id"]

        # 1) La orden pasa a estado "Delivering"
        status = "Delivering"
        db_delivery = await delivery_service.update_delivery_status(order_id, status)
        await publish_order_delivered(order_id=order_id, status=status)

        logger.info(f"[DELIVERY] 🚚 Comenzando entrega para orden: {order_id}")
        await publish_to_logger(
            message={
                "message": "Comenzando entrega para orden",
                "order_id": order_id,
                "status": status,
            },
            topic="delivery.info",
        )

        # 2) Se realiza la entrega y se actualiza con el estado final
        status = await delivery_service.deliver(order_id=order_id)
        await delivery_service.update_delivery_status(order_id, status)
        await publish_order_delivered(order_id=order_id, status=status)

        logger.info(f"[DELIVERY] ✅ Entrega completada para orden: {order_id} (status={status})")
        await publish_to_logger(
            message={
                "message": "Entrega completada para orden",
                "order_id": order_id,
                "status": status,
            },
            topic="delivery.info",
        )


async def consume_auth_events():
    _, channel = await get_channel()
    
    exchange = await declare_exchange(channel)
    
    delivery_queue = await channel.declare_queue('delivery_queue', durable=True)
    await delivery_queue.bind(exchange, routing_key="auth.running")
    await delivery_queue.bind(exchange, routing_key="auth.not_running")
    
    await delivery_queue.consume(handle_auth_events)
    logger.info("[DELIVERY] 🟢 Escuchando eventos de auth (running/not_running)...")
    await publish_to_logger(
        message={"message": "🟢 Delivery escuchando eventos de auth"},
        topic="delivery.info",
    )

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
                    
                    logger.info(f"[DELIVERY] ✅ Clave pública de Auth guardada en {PUBLIC_KEY_PATH}")
                    await publish_to_logger(
                        message={
                            "message": "Clave pública de Auth guardada",
                            "path": PUBLIC_KEY_PATH,
                        },
                        topic="delivery.info",
                    )
            except Exception as exc:
                print(exc)
                logger.error(f"[DELIVERY] ❌ Error obteniendo clave pública de Auth: {exc}")
                await publish_to_logger(
                    message={
                        "message": "Error obteniendo clave pública de Auth",
                        "error": str(exc),
                    },
                    topic="delivery.error",
                )


async def publish_order_delivered(order_id,status):
    connection, channel = await get_channel()
    
    exchange = await declare_exchange(channel)
    await exchange.publish(
        Message(body=json.dumps({"order_id": order_id, "status":status}).encode()),
        routing_key="delivery.ready"
    )
    logger.info(f"[ORDER] 📤 Publicado evento order.created → {order_id}")
    await publish_to_logger(
        message={
            "message": "Publicado evento delivery.ready",
            "order_id": order_id,
            "status": status,
        },
        topic="delivery.debug",   # o delivery.info 
    )
    await connection.close()

async def publish_to_logger(message: dict, topic: str):
    """
    Envía un log estructurado al sistema de logs.
    """
    connection = None
    try:
        connection, channel = await get_channel()

        # Exchange específico de logs
        exchange = await declare_exchange_logs(channel)

        # Estructura común de los logs
        log_data = {
            "measurement": "logs",
            "service": topic.split('.')[0],   # 'delivery'
            "severity": topic.split('.')[1],  # 'info', 'error', 'debug'...
            **message
        }

        msg = Message(
            body=json.dumps(log_data).encode(),
            content_type="application/json",
            delivery_mode=2,  # persistente
        )

        await exchange.publish(message=msg, routing_key=topic)

    except Exception as e:
        print(f"Error publishing to logger: {e}")
    finally:
        if connection:
            await connection.close()

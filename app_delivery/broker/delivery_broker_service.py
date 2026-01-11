# -*- coding: utf-8 -*-
"""Broker RabbitMQ del microservicio Delivery.

Responsabilidades:
    - Consumir eventos del exchange general:
        * order.fabricated           -> inicia entrega y publica delivery.finished
        * auth.running/not_running -> descarga clave pública de Auth
    - Consumir comandos del exchange_command:
        * cmd.check.delivery        -> responde evt.delivery.checked (exchange_saga)
    - Publicar:
        * delivery.finished        (exchange general)
        * evt.delivery.checked       (exchange_saga)
        * logs estructurados    (exchange_logs)
"""

from __future__ import annotations

import asyncio
import json
import logging

import httpx
from aio_pika import Message

from consul_client import get_service_url
from microservice_chassis_grupo2.core.rabbitmq_core import (
    PUBLIC_KEY_PATH,
    declare_exchange,
    declare_exchange_command,
    declare_exchange_logs,
    declare_exchange_saga,
    get_channel,
)
from services import delivery_service

logger = logging.getLogger(__name__)

# =============================================================================
# Constantes RabbitMQ (routing keys / colas / topics)
# =============================================================================

# --- Exchange general: eventos ---
RK_EVT_ORDER_READY = "order.fabricated"
RK_EVT_DELIVERY_READY = "delivery.finished"

RK_EVT_AUTH_RUNNING = "auth.running"
RK_EVT_AUTH_NOT_RUNNING = "auth.not_running"

QUEUE_ORDER_READY = "order_ready_queue"
QUEUE_AUTH_EVENTS = "delivery_queue"  # nombre histórico; lo mantenemos

# --- Exchange command: comandos ---
RK_CMD_CHECK_DELIVERY = "cmd.check.delivery"
QUEUE_CHECK_DELIVERY = "check_delivery_queue"

# --- Exchange saga: resultados hacia el orquestador ---
RK_SAGA_DELIVERY_RESULT = "evt.delivery.checked"

# --- Topics de logs ---
TOPIC_INFO = "delivery.info"
TOPIC_ERROR = "delivery.error"
TOPIC_DEBUG = "delivery.debug"

# --- Reglas de negocio de "check delivery"
DELIVERABLE_ADDRESSES = {"01", "20", "48"}


# =============================================================================
# Helpers internos (evitan duplicación)
# =============================================================================
#region 0. HELPERS
def _build_json_message(payload: dict) -> Message:
    """Construye un Message JSON persistente (recomendado para eventos/comandos).

    Reglas:
        - content_type='application/json'
        - delivery_mode=2 (persistente)
    """
    return Message(
        body=json.dumps(payload).encode(),
        content_type="application/json",
        delivery_mode=2,
    )


async def _publish_exchange(exchange, routing_key: str, payload: dict) -> None:
    """Publica payload JSON al exchange indicado con routing_key."""
    await exchange.publish(_build_json_message(payload), routing_key=routing_key)


def _require_fields(data: dict, required: tuple[str, ...], context: str) -> bool:
    """Valida que existan campos obligatorios en el payload.

    Devuelve:
        True si ok, False si falta alguno (y loggea el error).
    """
    missing = [k for k in required if data.get(k) is None]
    if not missing:
        return True
    logger.error("[DELIVERY] ❌ Payload inválido en %s, faltan %s: %s", context, missing, data)
    return False


# =============================================================================
# Handlers (consumidores)
# =============================================================================
#region 1. HANDLERS
async def handle_order_events(message) -> None:
    """Handler para order.fabricated.

    Flujo (idéntico al original, pero robusto):
        1) Marca status = 'Delivering' y publica delivery.finished (status Delivering)
        2) Ejecuta delivery_service.deliver()
        3) Actualiza el status final y vuelve a publicar delivery.finished (status final)
    """
    async with message.process():
        data = json.loads(message.body)

        if not _require_fields(data, ("order_id",), context=RK_EVT_ORDER_READY):
            await publish_to_logger(
                {"message": "Payload inválido en order.fabricated", "payload": data},
                TOPIC_ERROR,
            )
            return

        order_id = int(data["order_id"])

        # 1) Estado inicial de entrega
        status = "Delivering"
        await delivery_service.update_delivery_status(order_id, status)
        await publish_order_delivered(order_id=order_id, status=status)

        logger.info("[DELIVERY] 🚚 Comenzando entrega para order_id=%s", order_id)
        await publish_to_logger(
            {"message": "Comenzando entrega", "order_id": order_id, "status": status},
            TOPIC_INFO,
        )

        # 2) Ejecutar entrega
        final_status = await delivery_service.deliver(order_id=order_id)

        # 3) Persistir y publicar resultado (mantenemos el mismo evento delivery.finished)
        await delivery_service.update_delivery_status(order_id, final_status)
        await publish_order_delivered(order_id=order_id, status=final_status)

        logger.info("[DELIVERY] ✅ Entrega finalizada para order_id=%s con status=%s", order_id, final_status)
        await publish_to_logger(
            {"message": "Entrega completada", "order_id": order_id, "status": final_status},
            TOPIC_INFO,
        )


async def handle_auth_events(message) -> None:
    """Handler para auth.running / auth.not_running.

    - Solo cuando status == 'running' descarga la public key desde Auth via Consul.
    - Guarda el contenido en PUBLIC_KEY_PATH.
    """
    async with message.process():
        data = json.loads(message.body)

        status = data.get("status")
        logger.info("[DELIVERY] 📥 Evento auth recibido: %s", data)

        # En tu versión original solo actuabas si running
        if status != "running":
            return

        try:
            auth_service_url = await get_service_url("auth")
            logger.info("[DELIVERY] 🔍 Auth descubierto via Consul: %s", auth_service_url)

            async with httpx.AsyncClient() as client:
                response = await client.get(f"{auth_service_url}/auth/public-key")
                response.raise_for_status()
                public_key = response.text

            with open(PUBLIC_KEY_PATH, "w", encoding="utf-8") as f:
                f.write(public_key)

            logger.info("[DELIVERY] ✅ Clave pública guardada en %s", PUBLIC_KEY_PATH)
            await publish_to_logger(
                {"message": "Clave pública de Auth guardada", "path": PUBLIC_KEY_PATH},
                TOPIC_INFO,
            )
        except Exception as exc:
            logger.error("[DELIVERY] ❌ Error obteniendo clave pública: %s", exc, exc_info=True)
            await publish_to_logger(
                {"message": "Error obteniendo clave pública de Auth", "error": str(exc)},
                TOPIC_ERROR,
            )


async def handle_check_delivery(message) -> None:
    """Handler del comando cmd.check.delivery.

    Reglas (idénticas al original):
        - Si address está en {"01","20","48"} -> deliverable
        - Si no -> not_deliverable
    Publica el resultado en exchange_saga con routing key evt.delivery.checked.
    """
    async with message.process():
        data = json.loads(message.body)

        if not _require_fields(data, ("order_id", "address"), context=RK_CMD_CHECK_DELIVERY):
            await publish_to_logger(
                {"message": "Payload inválido en cmd.check.delivery", "payload": data},
                TOPIC_ERROR,
            )
            return

        order_id = int(data["order_id"])
        address = str(data["address"])

        status = "deliverable" if address in DELIVERABLE_ADDRESSES else "not_deliverable"
        await publish_delivery_result(order_id=order_id, status=status)

        logger.info("[DELIVERY] 📦 cmd.check.delivery order_id=%s address=%s -> %s", order_id, address, status)
        await publish_to_logger(
            {"message": "Resultado cmd.check.delivery", "order_id": order_id, "address": address, "status": status},
            TOPIC_DEBUG,
        )


# =============================================================================
# Consumers (setup colas + bindings)
# =============================================================================
#region 2. CONSUMERS
async def consume_order_events() -> None:
    """Consumer para eventos order.fabricated (exchange general)."""
    connection = None
    try:
        logger.info("[DELIVERY] 🔄 Iniciando consume_order_events...")
        connection, channel = await get_channel()
        exchange = await declare_exchange(channel)

        queue = await channel.declare_queue(QUEUE_ORDER_READY, durable=True)
        await queue.bind(exchange, routing_key=RK_EVT_ORDER_READY)
        await queue.consume(handle_order_events)

        logger.info("[DELIVERY] 🟢 Escuchando %s en cola %s", RK_EVT_ORDER_READY, QUEUE_ORDER_READY)
        await publish_to_logger(
            {"message": "Delivery escuchando eventos", "routing_key": RK_EVT_ORDER_READY, "queue": QUEUE_ORDER_READY},
            TOPIC_INFO,
        )

        await asyncio.Future()
    except Exception as exc:
        logger.error("[DELIVERY] ❌ Error en consume_order_events: %s", exc, exc_info=True)
        await publish_to_logger(
            {"message": "Error en consume_order_events", "error": str(exc)},
            TOPIC_ERROR,
        )
        raise
    finally:
        # Nota: en la práctica nunca llega aquí porque asyncio.Future() bloquea,
        # pero lo dejamos correcto por limpieza.
        if connection:
            await connection.close()


async def consume_auth_events() -> None:
    """Consumer para eventos de auth.running / auth.not_running (exchange general)."""
    connection = None
    try:
        logger.info("[DELIVERY] 🔄 Iniciando consume_auth_events...")
        connection, channel = await get_channel()
        exchange = await declare_exchange(channel)

        queue = await channel.declare_queue(QUEUE_AUTH_EVENTS, durable=True)
        await queue.bind(exchange, routing_key=RK_EVT_AUTH_RUNNING)
        await queue.bind(exchange, routing_key=RK_EVT_AUTH_NOT_RUNNING)
        await queue.consume(handle_auth_events)

        logger.info("[DELIVERY] 🟢 Escuchando auth.* en cola %s", QUEUE_AUTH_EVENTS)
        await publish_to_logger(
            {"message": "Delivery escuchando eventos auth", "queue": QUEUE_AUTH_EVENTS},
            TOPIC_INFO,
        )

        await asyncio.Future()
    except Exception as exc:
        logger.error("[DELIVERY] ❌ Error en consume_auth_events: %s", exc, exc_info=True)
        await publish_to_logger(
            {"message": "Error en consume_auth_events", "error": str(exc)},
            TOPIC_ERROR,
        )
        raise
    finally:
        if connection:
            await connection.close()


async def consume_check_delivery() -> None:
    """Consumer del comando cmd.check.delivery (exchange_command)."""
    connection = None
    try:
        connection, channel = await get_channel()
        exchange = await declare_exchange_command(channel)

        queue = await channel.declare_queue(QUEUE_CHECK_DELIVERY, durable=True)
        await queue.bind(exchange, routing_key=RK_CMD_CHECK_DELIVERY)
        await queue.consume(handle_check_delivery)

        logger.info("[DELIVERY] 🟢 Escuchando %s en cola %s", RK_CMD_CHECK_DELIVERY, QUEUE_CHECK_DELIVERY)
        await publish_to_logger(
            {"message": "Delivery escuchando comando", "routing_key": RK_CMD_CHECK_DELIVERY, "queue": QUEUE_CHECK_DELIVERY},
            TOPIC_INFO,
        )

        await asyncio.Future()
    except Exception as exc:
        logger.error("[DELIVERY] ❌ Error en consume_check_delivery: %s", exc, exc_info=True)
        await publish_to_logger(
            {"message": "Error en consume_check_delivery", "error": str(exc)},
            TOPIC_ERROR,
        )
        raise
    finally:
        if connection:
            await connection.close()


# =============================================================================
# Publishers (eventos)
# =============================================================================
#region 3. PUBLISHERS
async def publish_order_delivered(order_id: int, status: str) -> None:
    """Publica delivery.finished en el exchange general.

    Nota:
        - Mantengo el evento 'delivery.finished' exactamente como en tu fichero original.
        - Solo limpio logs y hago el mensaje persistente/JSON formal.
    """
    connection = None
    try:
        connection, channel = await get_channel()
        exchange = await declare_exchange(channel)

        payload = {"order_id": int(order_id), "status": str(status)}
        await _publish_exchange(exchange, RK_EVT_DELIVERY_READY, payload)

        logger.info("[DELIVERY] 📤 Publicado %s → %s", RK_EVT_DELIVERY_READY, payload)
        await publish_to_logger(
            {"message": "Publicado evento delivery.finished", "order_id": int(order_id), "status": str(status)},
            TOPIC_DEBUG,
        )
    finally:
        if connection:
            await connection.close()


async def publish_delivery_result(order_id: int, status: str) -> None:
    """Publica evt.delivery.checked en exchange_saga (resultado de cmd.check.delivery)."""
    connection = None
    try:
        connection, channel = await get_channel()
        exchange = await declare_exchange_saga(channel)

        payload = {"order_id": int(order_id), "status": str(status)}
        await _publish_exchange(exchange, RK_SAGA_DELIVERY_RESULT, payload)

        logger.info("[DELIVERY] 📤 Publicado %s → %s", RK_SAGA_DELIVERY_RESULT, payload)
    finally:
        if connection:
            await connection.close()


# =============================================================================
# Logger publisher (igual concepto que en Payment)
# =============================================================================
#region LOGGER
async def publish_to_logger(message: dict, topic: str) -> None:
    """Envía un log estructurado al exchange de logs.

    Args:
        message: contenido del log (dict)
        topic: routing key tipo 'delivery.info' | 'delivery.error' | 'delivery.debug'
    """
    connection = None
    try:
        connection, channel = await get_channel()
        exchange = await declare_exchange_logs(channel)

        service, severity = (topic.split(".", 1) + ["info"])[:2]

        log_data = {
            "measurement": "logs",
            "service": service,
            "severity": severity,
            **message,
        }

        await _publish_exchange(exchange, topic, log_data)
    except Exception:
        logger.exception("[DELIVERY] Error publicando log estructurado")
    finally:
        if connection:
            await connection.close()
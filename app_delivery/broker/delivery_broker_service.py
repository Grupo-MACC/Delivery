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
import os

import httpx
from aio_pika import Message

from consul_client import get_consul_client
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

def _internal_ca_file() -> str:
    """
    Devuelve la ruta del CA bundle para llamadas internas HTTPS.

    Por qué:
        - Los microservicios están usando certificados firmados por una CA privada.
        - httpx por defecto valida contra el bundle del sistema/certifi.
        - Si no le pasas tu CA, obtendrás CERTIFICATE_VERIFY_FAILED.

    Prioridad:
        1) INTERNAL_CA_FILE
        2) CONSUL_CA_FILE
        3) /certs/ca.pem (convención del proyecto)
    """
    return os.getenv("INTERNAL_CA_FILE") or os.getenv("CONSUL_CA_FILE") or "/certs/ca.pem"

async def _download_auth_public_key(auth_base_url: str) -> str:
    """
    Descarga la clave pública de Auth usando HTTPS con verificación por CA privada.

    Args:
        auth_base_url: Base URL (p.ej. "https://auth:5004")

    Returns:
        El texto PEM de la clave pública.

    Nota:
        - Separar esta función facilita reintentos.
    """
    async with httpx.AsyncClient(verify=_internal_ca_file(), timeout=5.0) as client:
        resp = await client.get(f"{auth_base_url}/auth/public-key")
        resp.raise_for_status()
        return resp.text


async def _ensure_auth_public_key(max_attempts: int = 20, base_delay: float = 0.25) -> None:
    """
    Asegura que existe la clave pública de Auth en PUBLIC_KEY_PATH.

    Estrategia simple:
        - Intenta resolver Auth por Consul (passing=true).
        - Si aún no hay instancias passing (race al arrancar), reintenta con backoff.
        - Cuando lo resuelve, descarga la clave con TLS verify (CA privada) y la guarda.

    Por qué:
        - auth.running se publica antes de que Auth esté realmente "ready" (FastAPI aún no sirve HTTP).
        - Por tanto, al recibir el evento, Consul puede devolver 0 passing temporalmente.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            auth_base_url = await get_consul_client().get_service_base_url("auth")
            public_key = await _download_auth_public_key(auth_base_url)

            # Escritura directa (simple). Si quieres más robustez: escribir a .tmp y renombrar.
            with open(PUBLIC_KEY_PATH, "w", encoding="utf-8") as f:
                f.write(public_key)

            logger.info("[DELIVERY] ✅ Clave pública de Auth guardada en %s", PUBLIC_KEY_PATH)
            return

        except Exception as exc:
            # OJO: esto NO es un error grave. Es normal durante el arranque.
            logger.warning(
                "[DELIVERY] ⏳ Auth aún no está 'passing' o no responde. Reintento %s/%s. Motivo: %s",
                attempt, max_attempts, exc
            )

            # Backoff suave (capado)
            delay = min(2.0, base_delay * (2 ** (attempt - 1)))
            await asyncio.sleep(delay)

    raise RuntimeError("No se pudo obtener la clave pública de Auth tras varios reintentos.")

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
    """
    Gestiona eventos de auth.running / auth.not_running.

    Nota importante:
        - Aunque recibamos 'running', Auth puede no estar listo aún (FastAPI aún no sirve HTTP).
        - Por eso hacemos reintentos contra Consul (passing=true) y luego descargamos la clave.
    """
    async with message.process():
        data = json.loads(message.body)
        if data.get("status") != "running":
            return

        try:
            await _ensure_auth_public_key()
            await publish_to_logger(
                message={"message": "Clave pública guardada", "path": PUBLIC_KEY_PATH},
                topic=TOPIC_INFO,
            )
        except Exception as exc:
            logger.error("[DELIVERY] ❌ Error obteniendo clave pública: %s", exc)
            await publish_to_logger(
                message={"message": "Error clave pública", "error": str(exc)},
                topic=TOPIC_ERROR,
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

async def ensure_auth_public_key(
    max_attempts: int = 30,
    sleep_seconds: float = 1.0,
) -> None:
    """
    Asegura que existe la clave pública de Auth en disco antes de validar JWT.

    Por qué existe esta función:
        - El evento `auth.running` NO es fiable (se puede perder si el consumer no estaba listo).
        - Si la public key no está, cualquier endpoint con get_current_user() cae con 401.

    Estrategia:
        1) Si el fichero ya existe y parece PEM válido, no hacemos nada.
        2) Descubrimos Auth (Consul) y pedimos /auth/public-key con reintentos.
        3) Guardamos de forma atómica (write tmp + os.replace) para evitar lecturas a medio escribir.
    """
    # 1) Si ya está, salimos
    if os.path.exists(PUBLIC_KEY_PATH):
        try:
            with open(PUBLIC_KEY_PATH, "r", encoding="utf-8") as f:
                content = f.read()
            if "BEGIN PUBLIC KEY" in content:
                return
        except Exception:
            # Si no se puede leer, forzamos re-descarga
            pass

    # Asegurar directorio
    dir_path = os.path.dirname(PUBLIC_KEY_PATH)
    if dir_path:
        os.makedirs(dir_path, exist_ok=True)

    last_exc: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            auth_service_url = await get_service_url("auth", default_url="http://auth:5004")

            async with httpx.AsyncClient(timeout=5.0) as client:
                r = await client.get(f"{auth_service_url}/auth/public-key")
                r.raise_for_status()
                public_key = r.text

            if "BEGIN PUBLIC KEY" not in public_key:
                raise ValueError("Auth devolvió una clave que no parece PEM válido")

            tmp_path = f"{PUBLIC_KEY_PATH}.tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                f.write(public_key)

            os.replace(tmp_path, PUBLIC_KEY_PATH)

            logger.info("✅ Public key de Auth guardada en %s", PUBLIC_KEY_PATH)
            return

        except Exception as exc:
            last_exc = exc
            logger.warning(
                "⚠️ No se pudo obtener public key (intento %s/%s): %s",
                attempt, max_attempts, exc
            )
            await asyncio.sleep(sleep_seconds)

    raise RuntimeError(f"No se pudo obtener la public key de Auth: {last_exc}")

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
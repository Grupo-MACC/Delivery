# -*- coding: utf-8 -*-
"""Main file to start FastAPI application."""
import logging.config
import os
from contextlib import asynccontextmanager
import uvicorn
from fastapi import FastAPI
from routers import delivery_router
from microservice_chassis_grupo2.sql import database, models
from broker import delivery_broker_service, setup_rabbitmq
import asyncio
from consul_client import create_consul_client

# Configure logging ################################################################################
logging.config.fileConfig(os.path.join(os.path.dirname(__file__), "logging.ini"))
logger = logging.getLogger(__name__)


# App Lifespan #####################################################################################
@asynccontextmanager
async def lifespan(__app: FastAPI):
    """Lifespan context manager."""
    consul_client = create_consul_client()
    service_id = os.getenv("SERVICE_ID", "delivery-1")
    service_name = os.getenv("SERVICE_NAME", "delivery")
    service_port = int(os.getenv("SERVICE_PORT", 5002))

    try:
        logger.info("Starting up")
        
        # Register with Consul
        result = await consul_client.register_service(
            service_name=service_name,
            service_id=service_id,
            service_port=service_port,
            service_address=service_name,  # Docker DNS
            tags=["fastapi", service_name],
            meta={"version": "2.0.0"},
            health_check_url=f"http://{service_name}:{service_port}/docs"
        )
        logger.info(f"✅ Consul service registration: {result}")

        try:
            logger.info("Creating database tables")
            async with database.engine.begin() as conn:
                await conn.run_sync(models.Base.metadata.create_all)
        except Exception:
            logger.error(
                "Could not create tables at startup",
            )
            
        '''try:
            await setup_rabbitmq.setup_rabbitmq()
        except Exception as e:
            logger.error(f"Error configurando RabbitMQ: {e}")'''

        try:
            task_order = asyncio.create_task(delivery_broker_service.consume_order_events())
            task_auth = asyncio.create_task(delivery_broker_service.consume_auth_events())
            task_check = asyncio.create_task(delivery_broker_service.consume_check_delivery())
        except Exception as e:
            logger.error(f"Error lanzando payment broker service: {e}")


        yield
    finally:
        logger.info("Shutting down database")
        await database.engine.dispose()
        logger.info("Shutting down rabbitmq")
        task_order.cancel()
        task_auth.cancel()
        task_check.cancel()
        
        # Deregister from Consul
        result = await consul_client.deregister_service(service_id)
        logger.info(f"✅ Consul service deregistration: {result}")


# OpenAPI Documentation ############################################################################
APP_VERSION = os.getenv("APP_VERSION", "2.0.0")
logger.info("Running app version %s", APP_VERSION)

app = FastAPI(
    redoc_url=None,  # disable redoc documentation.
    version=APP_VERSION,
    servers=[{"url": "/", "description": "Development"}],
    license_info={
        "name": "MIT License",
        "url": "https://choosealicense.com/licenses/mit/",
    },
    lifespan=lifespan,
)

app.include_router(delivery_router.router)

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=5002, reload=True)
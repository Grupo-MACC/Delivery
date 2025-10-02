# -*- coding: utf-8 -*-
"""Main file to start FastAPI application."""
import logging.config
import os
from contextlib import asynccontextmanager
import uvicorn
from fastapi import FastAPI
from routers import delivery_router
# Configure logging ################################################################################
logging.config.fileConfig(os.path.join(os.path.dirname(__file__), "logging.ini"))
logger = logging.getLogger(__name__)


# App Lifespan #####################################################################################
@asynccontextmanager
async def lifespan(__app: FastAPI):
    """Lifespan context manager."""
    try:
        logger.info("Starting up")
        try:
            logger.info("Creating database tables")
            async with database.engine.begin() as conn:
                await conn.run_sync(models.Base.metadata.create_all)
        except Exception:
            logger.error(
                "Could not create tables at startup",
            )
        yield
    finally:
        logger.info("Shutting down database")
        await database.engine.dispose()


# OpenAPI Documentation ############################################################################
APP_VERSION = os.getenv("APP_VERSION", "2.0.0")
logger.info("Running app version %s", APP_VERSION)
DESCRIPTION = """
Monolithic manufacturing order application.
"""

tag_metadata = [
    {
        "name": "Delivery",
        "description": "Endpoints related to deliverys",
    },
    {
        "name": "Order",
        "description": "Endpoints to **CREATE**, **READ**, **UPDATE** or **DELETE** orders.",
    },
    {
        "name": "Piece",
        "description": "Endpoints **READ** piece information.",
    },
]

app = FastAPI(
    redoc_url=None,  # disable redoc documentation.
    title="FastAPI - Monolithic app",
    description=DESCRIPTION,
    version=APP_VERSION,
    servers=[{"url": "/", "description": "Development"}],
    license_info={
        "name": "MIT License",
        "url": "https://choosealicense.com/licenses/mit/",
    },
    openapi_tags=tag_metadata,
    #lifespan=lifespan,
)

app.include_router(delivery_router.router)

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=5002, reload=True)

#python -m uvicorn main:app --reload --port 5000
# -*- coding: utf-8 -*-
"""Functions that interact with the database."""
import logging
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from . import models

logger = logging.getLogger(__name__)

async def create_payment_from_schema(db: AsyncSession, payment) -> models.Payment:
    db_payment = models.Payment(
        order_id=payment.order_id,
        amount_minor=payment.amount_minor,
        currency=payment.currency,
        status=models.Payment.STATUS_INITIATED,
    )
    db.add(db_payment)
    await db.commit()
    await db.refresh(db_payment)
    return db_payment

async def update_payment_status(db: AsyncSession, payment_id, status):
    db_payment = await get_element_by_id(db, models.Payment, payment_id)
    if db_payment is not None:
        db_payment.status = status
        await db.commit()
        await db.refresh(db_payment)
    return db_payment

async def get_payment_by_order_id(db: AsyncSession, order_id: int):
    stmt = select(models.Payment).where(models.Payment.order_id == order_id)
    result = await db.execute(stmt)
    payment = result.scalars().first()
    return payment

async def get_payment_list(db: AsyncSession):
    """Load all the orders from the database."""
    return await get_list(db, models.Payment)

async def get_payment(db: AsyncSession, payment_id):
    """Load an order from the database."""
    stmt = select(models.Payment).where(models.Payment.id == payment_id)
    order = await get_element_statement_result(db, stmt)
    return order

async def create_or_update_delivery_status(db: AsyncSession, order_id: int, status: str):
    """Crea o actualiza el estado de entrega de un pedido."""
    stmt = select(models.DeliveryStatus).where(models.DeliveryStatus.order_id == order_id)
    result = await db.execute(stmt)
    delivery_status = result.scalars().first()

    if delivery_status:
        delivery_status.status = status
    else:
        delivery_status = models.DeliveryStatus(order_id=order_id, status=status)
        db.add(delivery_status)

    await db.commit()
    await db.refresh(delivery_status)
    return delivery_status


async def get_delivery_status(db: AsyncSession, order_id: int):
    """Obtiene el estado de entrega de un pedido."""
    stmt = select(models.DeliveryStatus).where(models.DeliveryStatus.order_id == order_id)
    result = await db.execute(stmt)
    return result.scalars().first()

# Generic functions ################################################################################
# READ
async def get_list(db: AsyncSession, model):
    """Retrieve a list of elements from database"""
    result = await db.execute(select(model))
    item_list = result.unique().scalars().all()
    return item_list


async def get_list_statement_result(db: AsyncSession, stmt):
    """Execute given statement and return list of items."""
    result = await db.execute(stmt)
    item_list = result.unique().scalars().all()
    return item_list


async def get_element_statement_result(db: AsyncSession, stmt):
    """Execute statement and return a single items"""
    result = await db.execute(stmt)
    item = result.scalar()
    return item


async def get_element_by_id(db: AsyncSession, model, element_id):
    """Retrieve any DB element by id."""
    if element_id is None:
        return None

    element = await db.get(model, element_id)
    return element
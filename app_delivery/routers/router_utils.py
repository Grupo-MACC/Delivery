# -*- coding: utf-8 -*-
"""Util/Helper functions for router definitions."""
import logging
from fastapi import HTTPException

ORDER_SERVICE_URL = "http://order:5000"


logger = logging.getLogger(__name__)


def raise_and_log_error(my_logger, status_code: int, message: str):
    """Raises HTTPException and logs an error."""
    my_logger.error(message)
    raise HTTPException(status_code, message)
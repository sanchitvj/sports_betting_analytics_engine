# betflow/kafka/handlers/retry_handler.py

import logging
from typing import Callable, Any
from time import sleep
import asyncio


class RetryHandler:
    """Handles retry logic for failed operations."""

    def __init__(self, max_retries: int = 3, initial_backoff: float = 1.0):
        self.max_retries = max_retries
        self.initial_backoff = initial_backoff
        self.logger = logging.getLogger(self.__class__.__name__)
        self._sleep = sleep
        self._async_sleep = asyncio.sleep

    @property
    def sleep(self):
        return self._sleep

    @sleep.setter
    def sleep(self, func):
        self._sleep = func

    @property
    def async_sleep(self):
        return self._async_sleep

    @async_sleep.setter
    def async_sleep(self, func):
        self._async_sleep = func

    def _calculate_delay(self, attempt: int) -> float:
        """Calculate delay using exponential backoff."""
        return self.initial_backoff * (2 ** (attempt - 1))
    
    @staticmethod
    def _get_func_name(func: Callable) -> str:
        """Get function name safely."""
        # For Mock objects, get the name directly from the mock
        if hasattr(func, 'name'):
            return str(func.name)  # Convert to string to ensure we get just the name
        # For regular functions
        if hasattr(func, '__name__'):
            return func.__name__
        # Fallback
        return str(func)

    async def retry_async(self, func: Callable[..., Any], *args, **kwargs) -> Any:
        """Retry async function with exponential backoff."""
        attempts = 0
        last_error = None
        func_name = self._get_func_name(func)

        while attempts < self.max_retries:
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                attempts += 1
                last_error = e
                self.logger.warning(
                    f"Attempt {attempts} failed for {func_name}: {e}"
                )

                if attempts < self.max_retries:
                    delay = self._calculate_delay(attempts)
                    await self._async_sleep(delay)
                    self.logger.info(f"Retrying in {delay} seconds...")

        raise Exception(
            f"{func_name} failed after {self.max_retries} attempts. "
            f"Last error: {last_error}"
        )

    def retry_sync(self, func: Callable[..., Any], *args, **kwargs) -> Any:
        """Retry synchronous function with exponential backoff."""
        attempts = 0
        last_error = None
        func_name = self._get_func_name(func)

        while attempts < self.max_retries:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                attempts += 1
                last_error = e
                self.logger.warning(
                    f"Attempt {attempts} failed for {func_name}: {e}"
                )

                if attempts < self.max_retries:
                    delay = self._calculate_delay(attempts)
                    self._sleep(delay)
                    self.logger.info(f"Retrying in {delay} seconds...")

        raise Exception(
            f"{func_name} failed after {self.max_retries} attempts. "
            f"Last error: {last_error}"
        )
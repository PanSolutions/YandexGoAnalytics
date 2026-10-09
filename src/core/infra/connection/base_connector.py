from __future__ import annotations

from abc import ABC, abstractmethod
from types import TracebackType
from typing import Literal, Self

from loguru import logger


class BaseConnectionService(ABC):
    """Base class for connection services used as context managers.

    Subclasses implement :meth:`connect` and :meth:`disconnect`; the context
    manager protocol calls them and tracks the state in :attr:`is_connected`.
    """

    def __init__(self) -> None:
        self.is_connected: bool = False

    @abstractmethod
    def connect(self) -> None:
        """Establish the connection."""

    @abstractmethod
    def disconnect(self) -> None:
        """Release the connection and its resources."""

    def __enter__(self) -> Self:
        """Connect and return the service itself."""
        logger.info(f"Entering connection context for {self.__class__.__name__}")
        self.connect()
        self.is_connected = True
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> Literal[False]:
        """Disconnect and log errors; never suppresses exceptions from the body.

        Returns:
            Always ``False``, so exceptions raised inside the ``with`` block
            propagate to the caller.
        """
        try:
            self.disconnect()
            self.is_connected = False
            logger.info(f"Exiting connection context for {self.__class__.__name__}")
        except Exception as e:
            logger.error(f"Error while disconnecting in context manager: {e}")

        if exc_val:
            logger.exception(f"Exception caught in connection context session: {exc_val}")
        return False

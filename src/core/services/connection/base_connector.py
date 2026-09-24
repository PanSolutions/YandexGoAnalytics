from abc import ABC, abstractmethod
from loguru import logger


class BaseConnectionService(ABC):

    def __init__(self):
        self.is_connected = False

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def disconnect(self):
        pass

    def __enter__(self):
        logger.info(f"Entering connection context for {self.__class__.__name__}")
        self.connect()
        self.is_connected = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.disconnect()
            self.is_connected = False
            logger.info(f"Exiting connection context for {self.__class__.__name__}")
        except Exception as e:
            logger.error(f"Error while disconnecting in context manager: {e}")

        if exc_val:
            logger.exception(
                f"❌ Exception caught in connection context session: {exc_val}"
            )
            return False

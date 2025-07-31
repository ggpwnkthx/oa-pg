from abc import ABC, abstractmethod
from typing import AsyncIterator

from ..models import InputAddress


class AddressSource(ABC):
    """Abstract base for address sources."""

    @abstractmethod
    def records(self) -> AsyncIterator[InputAddress]:
        """
        Asynchronously yield InputAddress items.
        """
        raise NotImplementedError
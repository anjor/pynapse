from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class SynapseError(Exception):
    component: str
    operation: str
    message: str
    cause: Optional[BaseException] = None

    def __str__(self) -> str:
        base = f"{self.component}.{self.operation}: {self.message}"
        if self.cause is not None:
            return f"{base} (cause: {self.cause})"
        return base


def create_error(component: str, operation: str, message: str, cause: Optional[BaseException] = None) -> SynapseError:
    return SynapseError(component=component, operation=operation, message=message, cause=cause)

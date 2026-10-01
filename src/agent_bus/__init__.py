"""A small, local-only signaling bus for agent harnesses."""

from .core import ActionableItem, Bus, BusError, InboxBatch, Message, project_id

__version__ = "0.4.0"

__all__ = [
    "ActionableItem", "Bus", "BusError", "InboxBatch", "Message",
    "project_id",
]

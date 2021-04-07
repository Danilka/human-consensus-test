from __future__ import annotations


class NodeExceptions:
    """Exceptions for the node class."""

    class NodeValueError(ValueError):
        """Custom value exception for Node class."""
        pass

    class NodeNotEnoughData(ValueError):
        """Exception that is raised when there is not enough information to do something."""
        pass

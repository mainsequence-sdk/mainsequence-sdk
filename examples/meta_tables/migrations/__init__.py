"""Provider-based Alembic migration example for MetaTables."""

from .provider import ExampleAlembicVersion, migration

__all__ = ["ExampleAlembicVersion", "migration"]

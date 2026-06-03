"""initial account asset tables

Revision ID: 0001
Revises:
Create Date: 2026-06-03 00:00:00.000000
"""

from collections.abc import Sequence

from alembic import op

from examples.meta_tables.platform_managed.account_asset import Account, Asset

revision: str = "0001"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    bind = op.get_bind()
    Account.__table__.create(bind)
    Asset.__table__.create(bind)


def downgrade() -> None:
    bind = op.get_bind()
    Asset.__table__.drop(bind)
    Account.__table__.drop(bind)

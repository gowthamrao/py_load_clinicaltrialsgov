"""add unique constraints to child tables

Revision ID: e4bc992b21e8
Revises: 306262f13a15
Create Date: 2025-09-08 12:19:56.004990

"""

from typing import Sequence, Union


# revision identifiers, used by Alembic.
revision: str = "e4bc992b21e8"
down_revision: Union[str, None] = "306262f13a15"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass

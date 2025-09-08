"""add last_updated_api_str to raw_studies

Revision ID: 306262f13a15
Revises: 88c6978d6685
Create Date: 2025-09-07 16:39:53.057469

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "306262f13a15"
down_revision: Union[str, Sequence[str], None] = "88c6978d6685"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "raw_studies",
        sa.Column("last_updated_api_str", sa.String(length=255), nullable=True),
    )
    op.add_column(
        "staging_raw_studies",
        sa.Column("last_updated_api_str", sa.String(length=255), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("staging_raw_studies", "last_updated_api_str")
    op.drop_column("raw_studies", "last_updated_api_str")

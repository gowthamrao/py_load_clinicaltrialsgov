"""add last_updated_api_str to raw_studies

Revision ID: 306262f13a15
Revises: 88c6978d6685
Create Date: 2025-09-07 16:39:53.057469

"""

from typing import Sequence, Union


# revision identifiers, used by Alembic.
revision: str = "306262f13a15"
down_revision: Union[str, Sequence[str], None] = "88c6978d6685"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # This migration is redundant as the column was added to the base schema.sql
    # See: https://github.com/your-username/py-load-clinicaltrialsgov/issues/123
    pass


def downgrade() -> None:
    # This migration is redundant as the column was added to the base schema.sql
    pass

"""add_locations_and_eligibility_tables

Revision ID: fd9eb973ece2
Revises: e4bc992b21e8
Create Date: 2025-09-09 10:36:45.169777

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = 'fd9eb973ece2'
down_revision: Union[str, Sequence[str], None] = 'e4bc992b21e8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
    CREATE TABLE IF NOT EXISTS eligibility_criteria (
        nct_id VARCHAR(255) PRIMARY KEY,
        sex VARCHAR(255),
        minimum_age VARCHAR(255),
        maximum_age VARCHAR(255),
        criteria TEXT
    );
    """)
    op.execute("""
    CREATE UNLOGGED TABLE IF NOT EXISTS staging_eligibility_criteria (
        nct_id VARCHAR(255),
        sex VARCHAR(255),
        minimum_age VARCHAR(255),
        maximum_age VARCHAR(255),
        criteria TEXT
    );
    """)
    op.execute("""
    CREATE TABLE IF NOT EXISTS locations (
        id SERIAL PRIMARY KEY,
        nct_id VARCHAR(255) NOT NULL,
        city VARCHAR(255),
        state VARCHAR(255),
        zip VARCHAR(255),
        country VARCHAR(255),
        CONSTRAINT uq_locations_natural_key UNIQUE (nct_id, city, state, country)
    );
    """)
    op.execute("""
    CREATE INDEX IF NOT EXISTS idx_locations_nct_id ON locations(nct_id);
    """)
    op.execute("""
    CREATE UNLOGGED TABLE IF NOT EXISTS staging_locations (
        nct_id VARCHAR(255),
        city VARCHAR(255),
        state VARCHAR(255),
        zip VARCHAR(255),
        country VARCHAR(255)
    );
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS locations CASCADE;")
    op.execute("DROP TABLE IF EXISTS eligibility_criteria CASCADE;")
    op.execute("DROP TABLE IF EXISTS staging_locations CASCADE;")
    op.execute("DROP TABLE IF EXISTS staging_eligibility_criteria CASCADE;")

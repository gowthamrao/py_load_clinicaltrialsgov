"""Create initial schema from schema.sql

Revision ID: 88c6978d6685
Revises:
Create Date: 2025-09-07 22:43:44.428771

"""

from typing import Sequence, Union

from alembic import op
import importlib.resources
from load_clinicaltrialsgov import sql


# revision identifiers, used by Alembic.
revision: str = "88c6978d6685"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """
    Initial migration to create all tables from the schema.sql file.
    """
    schema_sql = importlib.resources.read_text(sql, "schema.sql")
    op.execute(schema_sql)


def downgrade() -> None:
    """
    Drops all tables.
    """
    op.execute("DROP TABLE IF EXISTS staging_design_outcomes;")
    op.execute("DROP TABLE IF EXISTS staging_intervention_arm_groups;")
    op.execute("DROP TABLE IF EXISTS staging_interventions;")
    op.execute("DROP TABLE IF EXISTS staging_conditions;")
    op.execute("DROP TABLE IF EXISTS staging_sponsors;")
    op.execute("DROP TABLE IF EXISTS staging_studies;")
    op.execute("DROP TABLE IF EXISTS staging_raw_studies;")
    op.execute("DROP TABLE IF EXISTS load_history;")
    op.execute("DROP TABLE IF EXISTS dead_letter_queue;")
    op.execute("DROP TABLE IF EXISTS design_outcomes;")
    op.execute("DROP TABLE IF EXISTS intervention_arm_groups;")
    op.execute("DROP TABLE IF EXISTS interventions;")
    op.execute("DROP TABLE IF EXISTS conditions;")
    op.execute("DROP TABLE IF EXISTS sponsors;")
    op.execute("DROP TABLE IF EXISTS studies;")
    op.execute("DROP TABLE IF EXISTS raw_studies;")

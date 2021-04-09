# pylint: skip-file
"""us_id_backfill_vacated_sentences

Revision ID: bb987a1ec29e
Revises: 08bfc99a8d94
Create Date: 2020-11-12 20:53:43.544546

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "bb987a1ec29e"
down_revision = "08bfc99a8d94"
branch_labels = None
depends_on = None

UPDATE_QUERY = (
    "UPDATE {table_name} "
    "SET status = 'VACATED' "
    "WHERE state_code = 'US_ID' AND status_raw_text IN ('V', 'Q');"
)

DOWNGRADE_QUERY = (
    "UPDATE {table_name} "
    "SET status = 'COMPLETED' "
    "WHERE state_code = 'US_ID' AND status_raw_text IN ('V', 'Q');"
)

TABLES_TO_UPDATE = [
    "state_supervision_sentence",
    "state_supervision_sentence_history",
    "state_incarceration_sentence",
    "state_incarceration_sentence_history",
]


def upgrade() -> None:
    connection = op.get_bind()
    for table in TABLES_TO_UPDATE:
        update_query = UPDATE_QUERY.format(table_name=table)
        connection.execute(update_query)


def downgrade() -> None:
    connection = op.get_bind()
    for table in TABLES_TO_UPDATE:
        downgrade_query = DOWNGRADE_QUERY.format(table_name=table)
        connection.execute(downgrade_query)

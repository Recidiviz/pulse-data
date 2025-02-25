# pylint: skip-file
"""rename_v2

Revision ID: 53c16671011c
Revises: c07f0bb025b5
Create Date: 2021-05-12 08:59:47.814802

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "53c16671011c"
down_revision = "c07f0bb025b5"
branch_labels = None
depends_on = None

UPGRADE_QUERY = """
UPDATE direct_ingest_ingest_file_metadata
SET file_tag = SUBSTRING(file_tag, 1, LENGTH(file_tag) - 3)
WHERE region_code = 'US_ID' AND file_tag IN (
    'early_discharge_incarceration_sentence_deleted_rows_v2',
    'early_discharge_supervision_sentence_deleted_rows_v2'
);
"""

DOWNGRADE_QUERY = """
UPDATE direct_ingest_ingest_file_metadata
SET file_tag = CONCAT(file_tag, '_v2')
WHERE region_code = 'US_ID' AND file_tag IN (
    'early_discharge_incarceration_sentence_deleted_rows',
    'early_discharge_supervision_sentence_deleted_rows'
);
"""


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(UPGRADE_QUERY)


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(DOWNGRADE_QUERY)

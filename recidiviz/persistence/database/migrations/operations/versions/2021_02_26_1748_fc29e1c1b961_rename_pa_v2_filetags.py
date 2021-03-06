# pylint: skip-file
"""rename_pa_v2_filetags

Revision ID: fc29e1c1b961
Revises: ee473e839c06
Create Date: 2021-02-26 17:48:10.307194

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "fc29e1c1b961"
down_revision = "ee473e839c06"
branch_labels = None
depends_on = None


UPGRADE_QUERY = """
UPDATE direct_ingest_ingest_file_metadata
SET file_tag = SUBSTRING(file_tag, 1, LENGTH(file_tag) - 3)
WHERE region_code = 'US_PA' AND file_tag IN (
    'doc_person_info_v2',
    'dbo_Miscon_v2',
    'supervision_period_v2'
);
"""

DOWNGRADE_QUERY = """
UPDATE direct_ingest_ingest_file_metadata
SET file_tag = CONCAT(file_tag, '_v2')
WHERE region_code = 'US_PA' AND file_tag IN (
    'doc_person_info',
    'dbo_Miscon',
    'supervision_period'
);
"""


def upgrade():
    with op.get_context().autocommit_block():
        op.execute(UPGRADE_QUERY)


def downgrade():
    with op.get_context().autocommit_block():
        op.execute(DOWNGRADE_QUERY)

# pylint: skip-file
"""rename_pa_v2_filetags

Revision ID: 672091849dfe
Revises: 28878e457c4f
Create Date: 2021-07-20 15:43:45.725133

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "672091849dfe"
down_revision = "28878e457c4f"
branch_labels = None
depends_on = None


UPGRADE_QUERY = """
UPDATE direct_ingest_ingest_file_metadata
SET file_tag = SUBSTRING(file_tag, 1, LENGTH(file_tag) - 3)
WHERE region_code = 'US_PA' AND file_tag IN (
    'dbo_Offender_v2',
    'doc_person_info_v2',
    'person_external_ids_v2',
    'sci_incarceration_period_v2',
    'supervision_period_v2'
);
"""

DOWNGRADE_QUERY = """
UPDATE direct_ingest_ingest_file_metadata
SET file_tag = CONCAT(file_tag, '_v2')
WHERE region_code = 'US_PA' AND file_tag IN (
    'dbo_Offender_v2',
    'doc_person_info_v2',
    'person_external_ids_v2',
    'sci_incarceration_period_v2',
    'supervision_period_v2'
);
"""


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(UPGRADE_QUERY)


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(DOWNGRADE_QUERY)

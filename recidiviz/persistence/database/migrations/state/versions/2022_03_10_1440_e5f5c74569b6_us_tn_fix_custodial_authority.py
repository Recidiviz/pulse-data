# pylint: skip-file
"""us_tn_fix_custodial_authority

Revision ID: e5f5c74569b6
Revises: ca7c10134ded
Create Date: 2022-03-10 14:40:43.743771

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
from recidiviz.utils.string import StrictStringFormatter

revision = "e5f5c74569b6"
down_revision = "ca7c10134ded"
branch_labels = None
depends_on = None


RAW_TEXT_QUERY = """UPDATE {table_name}
SET custodial_authority_raw_text = CONCAT(COALESCE(custodial_authority_raw_text, 'NONE'), '-', facility)
WHERE state_code = 'US_TN';
"""

ENUM_QUERY = """UPDATE {table_name}
SET custodial_authority = 'COURT'
WHERE state_code = 'US_TN' and facility in ('019', '033', '046', '047', '054', '057', '075', '079', '082');
"""


DOWNGRADE_RAW_TEXT_QUERY = """UPDATE {table_name}
SET custodial_authority_raw_text = 
    CASE
        WHEN custodial_authority_raw_text LIKE 'NONE-%' THEN NULL
        ELSE SPLIT_PART(custodial_authority_raw_text,'-', 1)
    END
WHERE state_code = 'US_TN';
"""

DOWNGRADE_ENUM_QUERY = """UPDATE {table_name} SET custodial_authority = 
    (CASE WHEN facility = '079' THEN 'INTERNAL_UNKNOWN'::state_custodial_authority
        ELSE NULL
    END)
WHERE state_code = 'US_TN' and facility in ('019', '033', '046', '047', '054', '057', '075', '079', '082');
"""

INCARCERATION_PERIOD_TABLE_NAME = "state_incarceration_period"
INCARCERATION_PERIOD_HISTORY_TABLE_NAME = "state_incarceration_period_history"


def upgrade() -> None:
    with op.get_context().autocommit_block():
        for table_name in [
            INCARCERATION_PERIOD_TABLE_NAME,
            INCARCERATION_PERIOD_HISTORY_TABLE_NAME,
        ]:
            for update_query in [RAW_TEXT_QUERY, ENUM_QUERY]:
                op.execute(
                    StrictStringFormatter().format(
                        update_query,
                        table_name=table_name,
                    )
                )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        for table_name in [
            INCARCERATION_PERIOD_TABLE_NAME,
            INCARCERATION_PERIOD_HISTORY_TABLE_NAME,
        ]:
            for downgrade_query in [DOWNGRADE_RAW_TEXT_QUERY, DOWNGRADE_ENUM_QUERY]:
                op.execute(
                    StrictStringFormatter().format(
                        downgrade_query,
                        table_name=table_name,
                    )
                )

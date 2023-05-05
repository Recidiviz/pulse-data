# pylint: skip-file
"""fix_leading_zero_staff_id

Revision ID: 9893a1f0050c
Revises: 4dd0c6a3ccfc
Create Date: 2023-05-04 16:47:06.041204

"""
import sqlalchemy as sa
from alembic import op

from recidiviz.utils.string import StrictStringFormatter

# revision identifiers, used by Alembic.
revision = "9893a1f0050c"
down_revision = "4dd0c6a3ccfc"
branch_labels = None
depends_on = None

UPGRADE_QUERY_TEMPLATE = """
    UPDATE {table_name}
    SET {field_name} = CAST(CAST({field_name} AS INTEGER) AS VARCHAR)
    WHERE state_code = 'US_ND'
    AND {field_name} != CAST(CAST({field_name} AS INTEGER) AS VARCHAR)
"""


def upgrade() -> None:
    for field_name, table_name in [
        ("external_id", "state_agent"),
        ("contacting_staff_external_id", "state_supervision_contact"),
        ("supervising_officer_staff_external_id", "state_supervision_period"),
    ]:
        op.execute(
            StrictStringFormatter().format(
                UPGRADE_QUERY_TEMPLATE, field_name=field_name, table_name=table_name
            )
        )


def downgrade() -> None:
    pass

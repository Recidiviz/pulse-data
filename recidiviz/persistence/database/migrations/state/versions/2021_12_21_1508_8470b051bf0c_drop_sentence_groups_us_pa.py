# pylint: skip-file
"""drop_sentence_groups_us_pa

Revision ID: 8470b051bf0c
Revises: f021a21a3a3d
Create Date: 2021-12-21 15:08:37.443012

"""
import sqlalchemy as sa
from alembic import op

from recidiviz.utils.string import StrictStringFormatter

# revision identifiers, used by Alembic.

revision = "8470b051bf0c"
down_revision = "f021a21a3a3d"
branch_labels = None
depends_on = None


DELETE_QUERY = "DELETE FROM {table_name} WHERE state_code = 'US_PA';"

TABLE_NAMES = ["state_sentence_group_history", "state_sentence_group"]


def upgrade() -> None:
    for table_name in TABLE_NAMES:
        op.execute(
            StrictStringFormatter().format(
                DELETE_QUERY,
                table_name=table_name,
            )
        )


def downgrade() -> None:
    # This migration does not have a downgrade
    pass

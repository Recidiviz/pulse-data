# pylint: skip-file
"""clear_bad_charge_classifications

Revision ID: 4e3749fd0305
Revises: 6178a06afa16
Create Date: 2021-08-24 18:22:24.398018

"""
import sqlalchemy as sa
from alembic import op

from recidiviz.utils.string import StrictStringFormatter

# revision identifiers, used by Alembic.
revision = "4e3749fd0305"
down_revision = "6178a06afa16"
branch_labels = None
depends_on = None


UPDATE_QUERY = (
    "UPDATE {table_name} SET classification_type = NULL"
    " WHERE classification_type_raw_text IS NULL;"
)

TABLES_TO_UPDATE = ["state_charge", "state_charge_history"]


def upgrade() -> None:
    with op.get_context().autocommit_block():
        for table in TABLES_TO_UPDATE:
            op.execute(StrictStringFormatter().format(UPDATE_QUERY, table_name=table))


def downgrade() -> None:
    # Migration is lossy, can't be undone.
    pass

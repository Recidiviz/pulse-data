# pylint: skip-file
"""clear_mo_supoff_from_person

Revision ID: b23f1567bce5
Revises: 5af027f85d2e
Create Date: 2022-08-09 10:50:33.020600

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
from recidiviz.utils.string import StrictStringFormatter

revision = "b23f1567bce5"
down_revision = "5af027f85d2e"
branch_labels = None
depends_on = None

UPDATE_QUERY = (
    "UPDATE state_person SET supervising_officer_id = NULL "
    "WHERE state_code = 'US_MO';"
)


def upgrade() -> None:
    op.execute(StrictStringFormatter().format(UPDATE_QUERY))


def downgrade() -> None:
    # This migration does not have a downgrade
    pass
